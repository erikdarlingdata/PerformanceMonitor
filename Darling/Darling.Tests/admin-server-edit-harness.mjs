/* Runs the shipped Admin page (wwwroot/js/pages/admin.js) under Node on a fake DOM with a focus model and a recording
   fetch, and prints the result of a scenario as one line of JSON. AdminServerEditBehaviourTests starts it as
       node admin-server-edit-harness.mjs <path to wwwroot/js> <scenario>[:<param>]
   The WHOLE js folder is copied into a scratch folder and the page is imported from there as the browser would, so the
   real panels.js, charts.js, util.js and views-api.js run; only the DOM, fetch and the window events are stand-ins.
   A scenario named `pure` reads {"fn": "...", "args": [...]} from stdin (JSON, so Windows argument quoting never
   touches it), calls that export of admin.js and prints {"result": ...}; a number that is not finite prints as
   {"$number": "NaN"}. The other scenarios drive the page and print what it drew and asked for.
   Helpers a flow scenario builds on (all defined below): state, requests, removed, main, mountPage, all, byTag,
   buttons, byAttr, fire, clickText, typeInto, settle, leaveHash, text. */
process.env.TZ = "UTC";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [jsDir, scenarioArg] = process.argv.slice(-2);
const [scenario, scenarioParam] = scenarioArg.split(":");

/* ---------------------------------------------------------------- the DOM */

/* The focus model: `focus()` makes a CONNECTED, enabled node document.activeElement; taking a node out of its parent (removeChild,
   remove, replaceChildren, a textContent reset, or moving it with appendChild / insertBefore, which removes it first)
   sets activeElement to null when that node holds it, as a browser blurs it. Putting the same node back does not give the
   focus back. Every node taken out is pushed to `removed`, so a scenario can assert that no call took an ancestor of the
   form. `isConnected` is true only under a node flagged isRoot (document.body is). */
let activeElement = null;
const removed = [];
const windowHandlers = {};
const documentHandlers = {};

function detach(node) {
  const parent = node.parent;
  if (!parent) return;
  parent.children = parent.children.filter((x) => x !== node);
  node.parent = null;
  removed.push(node);
  if (activeElement && node.contains(activeElement)) activeElement = null;
}

function descendants(node, out = []) {
  for (const c of node.children) {
    out.push(c);
    descendants(c, out);
  }
  return out;
}

/* A compound selector made of a tag, #id, .class and [attr] / [attr=value] parts; whitespace is the descendant combinator. */
const selectorParts = (s) => s.match(/[#.]?[\w-]+|\[[^\]]+\]|\*/g) || [];
function matchesCompound(n, s) {
  for (const t of selectorParts(s)) {
    if (t[0] === "#") {
      if (n.attrs.id !== t.slice(1)) return false;
    } else if (t[0] === ".") {
      if (!n.classList.contains(t.slice(1))) return false;
    } else if (t[0] === "[") {
      const m = /^\[([\w-]+)(?:=["']?([^"'\]]*)["']?)?\]$/.exec(t);
      if (!m || !(m[1] in n.attrs) || (m[2] !== undefined && n.attrs[m[1]] !== m[2])) return false;
    } else if (t !== "*" && n.tag !== t) return false;
  }
  return true;
}

class FakeNode {
  constructor(tag, text) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.listeners = {};
    this.parent = null;
    this._text = text == null ? "" : String(text);
    this.className = "";
    this.dataset = {};
    this.style = {};
    this.value = "";
    this.checked = false;
    this.disabled = false;
    this.open = false;
    this.hidden = false;
    this.isRoot = false;
    const self = this;
    this.classList = {
      contains: (c) => self.className.split(/\s+/).includes(c),
      add: (...cs) => { for (const c of cs) if (!self.classList.contains(c)) self.className = (self.className + " " + c).trim(); },
      remove: (...cs) => { self.className = self.className.split(/\s+/).filter((x) => x && !cs.includes(x)).join(" "); },
      toggle: (c, force) => {
        const on = force === undefined ? !self.classList.contains(c) : !!force;
        if (on) self.classList.add(c); else self.classList.remove(c);
        return on;
      },
    };
  }
  get nodeType() { return this.tag === "#text" ? 3 : 1; }
  get parentNode() { return this.parent; }
  get parentElement() { return this.parent; }
  get childNodes() { return this.children; }
  get firstChild() { return this.children[0] || null; }
  get lastChild() { return this.children[this.children.length - 1] || null; }
  get nextSibling() { const i = this.parent ? this.parent.children.indexOf(this) : -1; return i >= 0 ? this.parent.children[i + 1] || null : null; }
  get previousSibling() { const i = this.parent ? this.parent.children.indexOf(this) : -1; return i > 0 ? this.parent.children[i - 1] : null; }
  get isConnected() { let n = this; while (n.parent) n = n.parent; return n.isRoot; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { for (const c of [...this.children]) detach(c); this._text = String(v); }
  get id() { return this.attrs.id || ""; }
  contains(n) { for (let x = n; x; x = x.parent) if (x === this) return true; return false; }
  focus() { if (this.isConnected && !this.disabled) activeElement = this; }
  blur() { if (activeElement === this) activeElement = null; }
  setAttribute(k, v) {
    this.attrs[k] = String(v);
    if (k === "value") this.value = String(v);
    if (k === "disabled" || k === "checked" || k === "open" || k === "hidden") this[k] = true;
  }
  getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; }
  hasAttribute(k) { return k in this.attrs; }
  removeAttribute(k) {
    delete this.attrs[k];
    if (k === "disabled" || k === "checked" || k === "open" || k === "hidden") this[k] = false;
  }
  appendChild(c) {
    if (c.tag === "#fragment") { for (const k of [...c.children]) this.appendChild(k); return c; }
    detach(c);
    c.parent = this;
    this.children.push(c);
    return c;
  }
  append(...nodes) { for (const n of nodes) this.appendChild(typeof n === "string" ? new FakeNode("#text", n) : n); }
  insertBefore(c, ref) {
    detach(c);
    c.parent = this;
    const i = ref ? this.children.indexOf(ref) : -1;
    if (i < 0) this.children.push(c); else this.children.splice(i, 0, c);
    return c;
  }
  removeChild(c) { if (c.parent === this) detach(c); return c; }
  remove() { detach(this); }
  replaceChildren(...nodes) { for (const c of [...this.children]) detach(c); this.append(...nodes); }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  removeEventListener(t, fn) { this.listeners[t] = (this.listeners[t] || []).filter((x) => x !== fn); }
  querySelectorAll(sel) {
    let pool = [this];
    for (const part of sel.trim().split(/\s+/)) {
      const next = [];
      for (const root of pool) for (const d of descendants(root)) if (matchesCompound(d, part) && !next.includes(d)) next.push(d);
      pool = next;
    }
    return pool;
  }
  querySelector(sel) { return this.querySelectorAll(sel)[0] || null; }
  getBoundingClientRect() { return { top: 0, left: 0, right: 0, bottom: 0, width: 0, height: 0 }; }
  scrollIntoView() {}
  select() {}
}

const body = new FakeNode("body");
body.isRoot = true;
const documentElement = new FakeNode("html");
globalThis.Node = FakeNode;
globalThis.document = {
  createElement: (t) => new FakeNode(t),
  createElementNS: (ns, t) => new FakeNode(t),
  createTextNode: (t) => new FakeNode("#text", t),
  createDocumentFragment: () => new FakeNode("#fragment"),
  get activeElement() { return activeElement; },
  body,
  documentElement,
  getElementById: (id) => body.querySelector("#" + id),
  querySelector: (s) => body.querySelector(s),
  querySelectorAll: (s) => body.querySelectorAll(s),
  addEventListener: (t, fn) => (documentHandlers[t] ||= []).push(fn),
  removeEventListener: (t, fn) => { documentHandlers[t] = (documentHandlers[t] || []).filter((x) => x !== fn); },
  cookie: "",
  hidden: false,
  visibilityState: "visible",
};
globalThis.window = globalThis;
globalThis.addEventListener = (t, fn) => (windowHandlers[t] ||= []).push(fn);
globalThis.removeEventListener = (t, fn) => { windowHandlers[t] = (windowHandlers[t] || []).filter((x) => x !== fn); };
globalThis.location = { hash: "#/admin/servers", pathname: "/", search: "", href: "http://localhost/", origin: "http://localhost" };
globalThis.history = { pushState() {}, replaceState() {}, back() {} };
const memory = () => {
  const m = new Map();
  return { getItem: (k) => (m.has(k) ? m.get(k) : null), setItem: (k, v) => m.set(k, String(v)), removeItem: (k) => m.delete(k) };
};
globalThis.localStorage = memory();
globalThis.sessionStorage = memory();
globalThis.matchMedia = () => ({ matches: false, addEventListener() {}, removeEventListener() {} });
globalThis.requestAnimationFrame = (fn) => setTimeout(() => fn(Date.now()), 0);
globalThis.cancelAnimationFrame = (id) => clearTimeout(id);
const confirms = [];
globalThis.confirm = (m) => { confirms.push(String(m)); return state.confirmAnswer; };

/* ---------------------------------------------------------------- the service */

/* What the fake service answers. `servers` is GET /api/admin/servers; `byId` maps a server id to the by-id read
   (GET /api/admin/servers/<id>); `responder(method, url, body)` answers every other request ({ status, body, raw? };
   `raw`, when given, is the response text as it is, a sign-in page or an empty body, instead of JSON). `listReply` and
   `byIdReply`, when set to a function, answer that read instead ({ status, body, raw? }). `gate` holds a write until it
   resolves and `listGate` holds a list read the same way. */
const state = {
  canEdit: true,
  confirmAnswer: true,
  servers: [],
  byId: {},
  listReply: null,
  byIdReply: null,
  responder: () => ({ status: 200, body: {} }),
  gate: null,
  listGate: null,
  networkDown: false,
};
/* Every request the page made, reads included, in order: { method, url, body, contentType }. */
const requests = [];
const reply = (status, payload, raw) => ({
  status,
  ok: status >= 200 && status < 300,
  text: async () => (raw !== undefined ? raw : JSON.stringify(payload)),
});
globalThis.fetch = async (url, init = {}) => {
  const method = (init.method || "GET").toUpperCase();
  const u = String(url);
  const sent = init.body ? JSON.parse(init.body) : null;
  requests.push({ method, url: u, body: sent, contentType: (init.headers || {})["Content-Type"] || null });
  if (u === "/api/session") return reply(200, { can_edit: state.canEdit });
  if (method === "GET" && u === "/api/admin/servers") {
    if (state.listGate) await state.listGate;
    const r = state.listReply ? state.listReply() : { status: 200, body: { server_count: state.servers.length, servers: state.servers } };
    return reply(r.status, r.body, r.raw);
  }
  const byIdMatch = /^\/api\/admin\/servers\/(\d+)$/.exec(u);
  if (method === "GET" && byIdMatch) {
    const r = state.byIdReply
      ? state.byIdReply(Number(byIdMatch[1]))
      : state.byId[byIdMatch[1]]
        ? { status: 200, body: state.byId[byIdMatch[1]] }
        : { status: 404, body: { error: "This server's definition no longer exists." } };
    return reply(r.status, r.body, r.raw);
  }
  if (state.networkDown) throw new Error("connection refused");
  if (state.gate) await state.gate;
  const r = state.responder(method, u, sent);
  return reply(r.status, r.body, r.raw);
};

/* ---------------------------------------------------------------- helpers */

const all = (node, pred, out = []) => {
  if (pred(node)) out.push(node);
  for (const c of node.children) all(c, pred, out);
  return out;
};
const byTag = (root, tag) => all(root, (n) => n.tag === tag);
const buttons = (root, text) => all(root, (n) => n.tag === "button" && (text === undefined || n.textContent === text));
/* A node carrying attribute `name` (a data-* key may be set as an attribute or through dataset); with `value`, equal to it. */
const byAttr = (root, name, value) => all(root, (n) => {
  const camel = name.replace(/^data-/, "").replace(/-([a-z])/g, (_, c) => c.toUpperCase());
  const v = name in n.attrs ? n.attrs[name] : name.startsWith("data-") && camel in n.dataset ? n.dataset[camel] : undefined;
  return v !== undefined && (value === undefined || v === String(value));
});
const text = (node) => node.textContent;
const settle = (ms = 15) => new Promise((r) => setTimeout(r, ms));
/* Fires `type` on the node's own handlers (no bubbling) and waits for them and for what they started. A click on a disabled
   node does nothing, as in a browser. */
const fire = async (node, type, e = {}) => {
  if (type === "click" && node.disabled) return;
  const event = { type, target: node, currentTarget: node, preventDefault() {}, stopPropagation() {}, ...e };
  for (const fn of node.listeners[type] || []) await fn(event);
  await settle();
};
const clickText = async (root, label) => {
  const b = buttons(root, label)[0];
  if (!b) throw new Error("no button " + label);
  await fire(b, "click");
};
/* What a user does: focus the box, change its value, and let the input handlers run. */
const typeInto = async (node, value) => {
  node.focus();
  node.value = value;
  await fire(node, "input");
};
/* Navigate away (or anywhere): sets the hash and runs the window's hashchange handlers. */
const leaveHash = async (hash) => {
  location.hash = hash;
  for (const fn of windowHandlers.hashchange || []) await fn({ type: "hashchange" });
  await settle();
};

/* ---------------------------------------------------------------- the page */

const readStdin = async () => {
  let s = "";
  for await (const chunk of process.stdin) s += chunk;
  return s;
};

/* The whole folder, subfolders included, never a list of modules that could go stale. */
const copyTree = (from, to) => {
  fs.mkdirSync(to, { recursive: true });
  for (const entry of fs.readdirSync(from, { withFileTypes: true })) {
    const source = path.join(from, entry.name);
    if (entry.isDirectory()) copyTree(source, path.join(to, entry.name));
    else fs.copyFileSync(source, path.join(to, entry.name));
  }
};

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "admin-server-edit-"));
let output = {};
try {
  copyTree(jsDir, scratch);
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const page = await import(pathToFileURL(path.join(scratch, "pages", "admin.js")).href);
  const util = await import(pathToFileURL(path.join(scratch, "util.js")).href);
  const main = body.appendChild(new FakeNode("main"));
  const mountPage = async (tab = "servers") => {
    page.renderAdmin(main, tab);
    await settle();
  };

  const server = (id, name, over = {}) => ({
    server_id: id, server_name: name.toLowerCase(), display_name: name, engine: "sqlserver", version: "SQL Server 2022",
    freshness: "Online", status: "Enabled", auth: "Windows", monthly_cost: "$1,234", monthly_cost_usd: 1234,
    added: "2025-12-31T00:00:00.0000000", read_only: false, last_collected: "2026-01-01T00:00:00Z", ...over,
  });
  const alpha = server(1, "Alpha");
  const bravo = server(2, "Bravo", {
    engine: "postgres", version: "PostgreSQL 18", freshness: "AwaitingFirstCollection", status: "Disabled", auth: "SQL Server",
    monthly_cost: null, monthly_cost_usd: 0, added: "2026-01-02T00:00:00.0000000", last_collected: null,
  });

  const scenarios = {
    /* The frame's proof: a read-only seat sees the Servers tab drawn by the real grid. */
    frame: async () => {
      state.canEdit = false;
      state.servers = [alpha, bravo];
      await mountPage("servers");
      const table = byTag(main, "table")[0];
      const headRow = table.children[0].children[0];
      const rows = table.children[1].children;
      const notice = all(main, (n) => n.className.split(/\s+/).includes("notice")).map(text);
      return {
        headers: headRow.children.map((th) => (th.children[0] || th).textContent),
        cells: rows.map((tr) => tr.children.map(text)),
        rowClasses: rows.map((tr) => tr.className),
        notice,
        editHeaders: headRow.children.filter((th) => (th.children[0] || th).textContent === "Edit").length,
        editButtons: buttons(main, "Edit").length,
        requests: requests.map((r) => r.method + " " + r.url),
        removedTotal: removed.length,
        activeElement: activeElement === null,
      };
    },
    /* The focus model itself, through the real mount(): a focused box survives a repaint that leaves its node attached and
       loses focus when an ancestor is cleared, even if the very same node is put back. */
    focusModel: async () => {
      const form = new FakeNode("form");
      const box = form.appendChild(new FakeNode("input"));
      const area = new FakeNode("div");
      const stage = body.appendChild(new FakeNode("div"));
      util.mount(stage, [form, area]);
      box.focus();
      const afterFocus = activeElement === box;
      util.mount(area, [new FakeNode("span", "list")]);
      const keptWhenSiblingRepaints = activeElement === box;
      const removedBefore = removed.length;
      util.mount(stage, [form, area]);
      const lostWhenAncestorCleared = activeElement === null;
      const sameNodeBack = form.isConnected && box.isConnected;
      const ancestorRecorded = removed.slice(removedBefore).includes(form);
      box.focus();
      stage.textContent = "";
      const lostOnTextReset = activeElement === null;
      const detached = new FakeNode("input");
      detached.focus();
      const detachedCannotFocus = activeElement === null;
      const off = body.appendChild(new FakeNode("input"));
      off.disabled = true;
      off.focus();
      const disabledCannotFocus = activeElement === null;
      return { afterFocus, keptWhenSiblingRepaints, lostWhenAncestorCleared, sameNodeBack, ancestorRecorded, lostOnTextReset, detachedCannotFocus, disabledCannotFocus };
    },
    /* One call of an admin.js export: {"fn": "buildEditBody", "args": [...]} on stdin. */
    pure: async () => {
      const { fn, args = [] } = JSON.parse(await readStdin());
      if (!(fn in page)) throw new Error("admin.js exports no " + fn);
      return { result: typeof page[fn] === "function" ? page[fn](...args) : page[fn] };
    },
  };
  if (!(scenario in scenarios)) throw new Error("unknown scenario " + scenario);
  output = await scenarios[scenario](scenarioParam);
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
const nonFinite = (k, v) => (typeof v === "number" && !Number.isFinite(v) ? { $number: String(v) } : v);
process.stdout.write(JSON.stringify(output, nonFinite) + "\n", () => process.exit(0));
