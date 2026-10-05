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
   `raw`, when given, is the response text as it is, a sign-in page or an empty body, instead of JSON). `listReply`,
   `byIdReply` and `sessionReply`, when set to a function, answer that read instead ({ status, body, raw? }); one that
   throws is a transport failure. `gate` holds a write until it resolves, `listGate` holds a list read the same way and
   `byIdGate` holds a by-id read. */
const state = {
  canEdit: true,
  confirmAnswer: true,
  servers: [],
  byId: {},
  listReply: null,
  byIdReply: null,
  sessionReply: null,
  responder: () => ({ status: 200, body: {} }),
  gate: null,
  listGate: null,
  byIdGate: null,
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
  if (u === "/api/session") {
    const r = state.sessionReply ? state.sessionReply() : { status: 200, body: { can_edit: state.canEdit } };
    return reply(r.status, r.body, r.raw);
  }
  if (method === "GET" && u === "/api/admin/servers") {
    if (state.listGate) await state.listGate;
    const r = state.listReply ? state.listReply() : { status: 200, body: { server_count: state.servers.length, servers: state.servers } };
    return reply(r.status, r.body, r.raw);
  }
  const byIdMatch = /^\/api\/admin\/servers\/(\d+)$/.exec(u);
  if (method === "GET" && byIdMatch) {
    if (state.byIdGate) await state.byIdGate;
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

  /* The by-id read (GET /api/admin/servers/<id>) as the service answers it: no secret, and modified_at, the opaque token. */
  const TOKEN = "2026-10-05T12:34:56.1234567Z";
  const read = (id, name, over = {}) => ({
    server_id: id, display_name: name, engine: "sqlserver", host: name.toLowerCase(), port: 0, database: null, read_only_intent: false,
    auth: "Windows", username: null, encrypt_mode: "Mandatory", trust_server_certificate: false, multi_subnet_failover: false,
    monthly_cost_usd: 1234, modified_at: TOKEN, ...over,
  });
  const charlie = server(3, "Charlie", { auth: "SQL Server" });
  const reads = {
    alpha: read(1, "Alpha"),
    bravo: read(2, "Bravo", { engine: "postgres", port: 5433, auth: "SQL", username: "pgmon", encrypt_mode: "Optional", monthly_cost_usd: 0 }),
    charlie: read(3, "Charlie", { auth: "SQL", username: "sa", database: "Sales", read_only_intent: true, trust_server_certificate: true }),
  };
  /* The editing seat's Servers tab: Alpha (Windows), Bravo (PostgreSQL) and Charlie (SQL login), each with its by-id read. */
  const seed = () => {
    state.servers = [alpha, bravo, charlie];
    state.byId = { 1: reads.alpha, 2: reads.bravo, 3: reads.charlie };
  };

  /* What the page drew. The three boxes carry data-box, the form data-edit-form, a field data-field (the API key), a row data-row. */
  const box = (name) => byAttr(main, "data-box", name)[0] || null;
  const openForm = () => byAttr(main, "data-edit-form")[0] || null;
  const field = (key) => byAttr(main, "data-field", key).find((n) => n.tag === "input" || n.tag === "select") || null;
  const clickEdit = async (id) => {
    const b = byAttr(main, "data-server-id", id).find((n) => n.tag === "button");
    if (!b) throw new Error("no Edit button for server " + id);
    await fire(b, "click");
  };
  const rowInfo = (key) => {
    const r = all(main, (n) => n.attrs["data-row"] === key)[0];
    return r ? { label: r.children[0] ? text(r.children[0]) : text(r), hidden: r.hidden, display: r.style.display || "" } : null;
  };
  const tableRows = () => {
    const t = byTag(main, "table")[0];
    return t ? t.children[1].children.map((tr) => (tr.children[0] ? text(tr.children[0]) : "")) : [];
  };
  const listGets = () => requests.filter((r) => r.method === "GET" && r.url === "/api/admin/servers").length;
  const byIdGets = () => requests.filter((r) => r.method === "GET" && /^\/api\/admin\/servers\/\d+$/.test(r.url)).map((r) => r.url);
  const strips = (name) => box(name).children.map((c) => ({ text: text(c), cls: c.className, role: c.attrs.role || null }));
  /* The open form as a user sees it: heading, which fields it has, what they hold, the notes, the buttons. */
  const formSnapshot = () => {
    const form = openForm();
    if (!form) return null;
    const heading = byTag(form, "h3")[0];
    const input = (key) => byAttr(form, "data-field", key).find((n) => n.tag === "input" || n.tag === "select") || null;
    const keys = [...new Set(byAttr(form, "data-field").map((n) => n.attrs["data-field"]))];
    const kind = (k) => (input(k) ? input(k).attrs.type : undefined);
    return {
      heading: text(heading),
      headingFocused: activeElement === heading,
      headingTabindex: heading.attrs.tabindex,
      keys,
      values: Object.fromEntries(keys.filter((k) => input(k) && !["checkbox", "radio"].includes(kind(k))).map((k) => [k, input(k).value])),
      checks: Object.fromEntries(keys.filter((k) => kind(k) === "checkbox").map((k) => [k, input(k).checked])),
      auth: byAttr(form, "data-field", "auth").map((r) => ({ value: r.attrs.value, checked: r.checked })),
      encryption: input("encrypt_mode") ? all(input("encrypt_mode"), (n) => n.tag === "option").map((o) => o.attrs.value) : [],
      engine: text(byAttr(form, "data-field", "engine")[0]),
      muted: all(form, (n) => n.className === "muted").map(text),
      buttons: buttons(form).map(text),
      passwordType: input("password").attrs.type,
      passwordValue: input("password").value,
    };
  };

  /* A click that reaches the node's handlers although the node is disabled, as a stale or synthetic event would (a real browser
     sends none to a disabled button): the proof that a guard in the code holds behind the disabled state. */
  const forceClick = async (node) => {
    for (const fn of node.listeners.click || []) await fn({ type: "click", target: node, currentTarget: node, preventDefault() {}, stopPropagation() {} });
    await settle();
  };
  /* Every piece of text the page drew, in document order (a node's own text, so a cell is one piece and a heading another). */
  const textsUnder = (root) => all(root, (n) => n._text !== "").map((n) => n._text);
  const SECRET = "SECRET-PW";
  /* How many nodes under the page hold `secret` in a value, an attribute or their text (an ancestor of one counts too). */
  const leaks = (secret) => all(main, (n) => n.value === secret || n.textContent.includes(secret) || Object.values(n.attrs).some((v) => String(v).includes(secret))).length;
  const secretNodes = () => leaks(SECRET);
  const patches = () => requests.filter((r) => r.method === "PATCH").map((r) => ({ url: r.url, body: r.body, contentType: r.contentType }));
  /* The 409 panel of an open form: the lines naming what the other edit changed (and the field each is about), the "none of the
     fields changed" sentence, and the buttons; null while there is no panel. */
  const conflictPanel = (form) => {
    const b = form ? byAttr(form, "data-box", "conflict")[0] : null;
    if (!b || !b.children.length) return null;
    const none = all(b, (n) => n.attrs["data-role"] === "conflict-none")[0];
    const items = all(b, (n) => n.tag === "li");
    return { lines: items.map(text), fields: items.map((n) => n.attrs["data-conflict-field"]), none: none ? text(none) : null, buttons: buttons(b).map(text) };
  };
  /* The Servers tab as a user sees it now, safe to take when its boxes are gone (another tab is on screen): the open form's
     banner, status line and 409 panel, whether Save and Cancel and each Edit button are disabled, the notice, and what was asked of the service. */
  const view = () => {
    const form = openForm();
    const strip = (key) => {
      const b = form ? byAttr(form, "data-box", key)[0] : null;
      return b ? b.children.map((c) => ({ text: text(c), cls: c.className, role: c.attrs.role || null })) : [];
    };
    const one = (label) => (form ? buttons(form, label)[0] : null);
    return {
      forms: byAttr(main, "data-edit-form").length,
      formBoxChildren: box("form") ? box("form").children.length : null,
      banner: strip("banner"),
      status: strip("status"),
      conflict: conflictPanel(form),
      saveDisabled: one("Save") ? one("Save").disabled : null,
      cancelDisabled: one("Cancel") ? one("Cancel").disabled : null,
      editDisabled: buttons(main, "Edit").map((b) => b.disabled),
      notice: box("notice") ? strips("notice") : [],
      patches: patches(),
      listReads: listGets(),
      byIdGets: byIdGets(),
      rows: tableRows(),
    };
  };
  /* The service's answer to a good save, as the route sends it. */
  const updated = (name, over = {}) => ({
    status: 200,
    body: { status: "updated", display_name: name, server: name.toLowerCase(), note: "Takes effect at the next collection cycle.", tested: false, ...over },
  });
  const renamed = (id, name) => {
    state.servers = state.servers.map((s) => (s.server_id === id ? { ...s, display_name: name, server_name: name.toLowerCase() } : s));
  };
  /* One edit on an open form: text into a box, a checkbox ticked or cleared, or a choice made in a list. */
  const edit = async (key, value) => {
    const node = field(key);
    if (typeof value === "boolean") {
      node.checked = value;
      await fire(node, "change");
    } else if (node.tag === "select") {
      node.value = value;
      await fire(node, "change");
    } else await typeInto(node, value);
  };
  /* Choose one of the four authentication radios, as a click does. */
  const chooseAuth = async (word) => {
    const radio = byAttr(main, "data-field", "auth").find((r) => r.attrs.value === word);
    if (!radio) throw new Error("no authentication choice " + word);
    radio.checked = true;
    await fire(radio, "change");
  };
  /* Save, with the PATCH held back at the service until the snapshot of "during" was taken; resolves to that snapshot. */
  const gatedSave = async () => {
    let release;
    state.gate = new Promise((r) => { release = r; });
    const click = clickText(main, "Save");
    await settle();
    const during = view();
    release();
    await click;
    await settle(30);
    state.gate = null;
    return during;
  };
  /* The modified_at another edit leaves behind, and the 409 conflict answer that carries the values it left (the by-id read's shape). */
  const TOKEN2 = "2026-10-05T13:00:00.7654321Z";
  const conflictAnswer = (current, message = "This server was changed since you read it; nothing was saved.") => ({ status: 409, body: { status: "conflict", message, current } });
  /* The save cases: which server's form, what the user changes, and what the service answers. `answer(body, n)` is called for the
     n-th PATCH (1 first) with the body it was sent; it may also change what the list read answers next. `typed` types SECRET into
     the password box; `down` makes the transport fail; `again` presses Save a second time after the first answer. */
  const SAVES = {
    nochange: { id: 1, edits: {}, answer: () => updated("Alpha") },
    connfail: { id: 3, edits: { host: "charlie-two" }, typed: true, answer: () => ({ status: 200, body: { status: "connection_failed", message: "Could not connect to charlie-two: login failed for user 'sa'." } }) },
    forbidden: { id: 1, edits: { display_name: "Alpha Two" }, answer: () => ({ status: 403, body: { error: "This account has read-only access." } }) },
    gone: {
      id: 1, edits: { display_name: "Alpha Two" },
      answer: () => {
        state.servers = state.servers.filter((s) => s.server_id !== 1);
        return { status: 404, body: { status: "not_found", message: "This server's definition no longer exists (removed since it was looked up); nothing was changed." } };
      },
    },
    updated: { id: 1, edits: { display_name: "Alpha Prime" }, answer: () => { renamed(1, "Alpha Prime"); return updated("Alpha Prime"); } },
    tested: { id: 3, edits: { host: "charlie-two" }, typed: true, answer: () => { renamed(3, "Charlie"); return updated("Charlie", { tested: true }); } },
    unchanged: { id: 1, edits: { display_name: "Alpha Two" }, answer: () => ({ status: 200, body: { status: "unchanged" } }) },
    invalid: { id: 3, edits: { host: "charlie two" }, typed: true, answer: () => ({ status: 400, body: { error: "The host 'charlie two' is not valid: SECRET-PW is not allowed here." } }) },
    clientcheck: { id: 3, edits: { host: "charlie-two", monthly_cost_usd: "abc" }, typed: true, answer: () => updated("Charlie") },
    nopassword: { id: 3, edits: { host: "charlie-two" }, answer: () => updated("Charlie") },
    limited: { id: 1, edits: { display_name: "Alpha Two" }, answer: () => ({ status: 429, body: { error: "Another server change is in progress. Try again in a moment." } }) },
    broken: { id: 1, edits: { display_name: "Alpha Two" }, answer: () => ({ status: 500, body: { error: "admin server edit failed (InvalidOperationException)" } }) },
    timedout: {
      id: 1, edits: { host: "alpha-two" }, again: true,
      answer: (body, n) => (n === 1
        ? { status: 503, body: { error: "The edit did not finish in time. It may still complete; reload to see." } }
        : updated("Alpha")),
    },
    network: { id: 1, edits: { display_name: "Alpha Two" }, down: true, answer: () => updated("Alpha Two") },
    expired: { id: 1, edits: { display_name: "Alpha Two" }, answer: () => ({ status: 200, raw: "<html><body>Sign in</body></html>" }) },
    collides: { id: 3, edits: { host: "alpha" }, typed: true, answer: () => ({ status: 409, body: { status: "collides", message: "Another server already uses the address alpha." } }) },
    conflict: { id: 1, edits: { display_name: "Alpha Two" }, answer: () => ({ status: 409, body: { status: "conflict", message: "changed", current: { ...reads.alpha, display_name: "Alpha Elsewhere" } } }) },
    // The answer is read by its status word, never by its message: a collision whose text talks about a change is no conflict, and a
    // conflict whose text talks about a collision still carries its panel.
    collidesTalk: { id: 3, edits: { host: "alpha" }, typed: true, answer: () => ({ status: 409, body: { status: "collides", message: "This server was changed since you opened it: another monitored server already uses the address." } }) },
    conflictTalk: { id: 3, edits: { host: "alpha" }, typed: true, answer: () => conflictAnswer({ ...reads.charlie, host: "charlie-new", modified_at: TOKEN2 }, "Another server already uses the address alpha.") },
    // The status line: which edits make the service test the connection (any connection change, any auth, or a password).
    probeWindowsHost: { id: 1, edits: { host: "alpha-two" }, answer: () => updated("Alpha") },
    probeWindowsTrust: { id: 1, edits: { trust_server_certificate: true }, answer: () => updated("Alpha") },
    probeWindowsEncrypt: { id: 1, edits: { encrypt_mode: "Strict" }, answer: () => updated("Alpha") },
    probeName: { id: 1, edits: { display_name: "Alpha Two" }, answer: () => updated("Alpha Two") },
    probeCost: { id: 1, edits: { monthly_cost_usd: "250" }, answer: () => updated("Alpha") },
    probeRotate: { id: 3, edits: {}, typed: true, answer: () => updated("Charlie") },
    probeSqlHost: { id: 3, edits: { host: "charlie-two" }, typed: true, answer: () => updated("Charlie") },
    probePostgresPort: { id: 2, edits: { port: "5434" }, typed: true, answer: () => updated("Bravo") },
  };
  /* The three tabs' payloads (the old vm harness's, kept as they were): each carries credential-shaped fields no cell may show. */
  const TAB_PAYLOADS = {
    servers: {
      server_count: 2,
      servers: [
        { server_name: "alpha", display_name: "Alpha", engine: "sqlserver", version: "SQL Server 2022", freshness: "Online", status: "Enabled", auth: "Windows",
          monthly_cost: "$1,234", monthly_cost_usd: 1234, added: "2025-12-31T00:00:00.0000000", read_only: false, last_collected: "2026-01-01T00:00:00Z", password: "SECRET-PW" },
        { server_name: "bravo", display_name: "Bravo", engine: "postgres", version: "PostgreSQL 18", freshness: "AwaitingFirstCollection", status: "Disabled", auth: "SQL Server",
          monthly_cost: null, monthly_cost_usd: 0, added: "2026-01-02T00:00:00.0000000", read_only: false, last_collected: null },
      ],
    },
    routes: {
      routes: [{
        route_id: 7, metric_match: "Blocking Detected", match_kind: "exact_metric", family: "performance", configured_channels: ["slack", "pagerduty"],
        smtp_recipients: ["ops@example.test"], enabled: true, modified_at_utc: "2026-01-01T00:00:00Z", webhook_url: "https://hooks.example.test/SECRET-HOOK",
        password: "SECRET-PW", routing_key: "SECRET-KEY", slack_url: "https://hooks.example.test/SECRET-SLACKURL", pagerduty_routing_key: "SECRET-PDKEY",
        slack_webhook_url: "https://hooks.example.test/SECRET-SLACK",
      }],
    },
    settings: {
      alerts_enabled: true, cooldown_minutes: 15,
      cpu: { enabled: true, threshold_percent: 90, webhook_url: "SECRET-HOOK", slack_url: "SECRET-SLACKURL", pagerduty_routing_key: "SECRET-PDKEY", connection_string: "SECRET-CS", auth_token: "SECRET-AUTH", future_knob: "UNLISTED-VALUE" },
      long_running_query: { enabled: true, threshold_minutes: 30, max_results: 10, exclude_backups: true, excluded_logins: ["svc"] },
      health_bands: { deadlock_warn_per_hour: 1, deadlock_critical_per_hour: 5 }, excluded_databases: ["master"],
      analysis: { enabled: true, interval_minutes: 60, smtp_password: "SECRET-PW" }, fleet_sweep: { enabled: true, interval_minutes: 60 },
      smtp_password: "SECRET-PW",
    },
  };
  const TAB_READS = { routes: "/api/read/get_notification_routes", settings: "/api/read/get_alert_settings" };
  /* A read-only seat whose tab reads answer those payloads (the Servers list through the list read, the others through the tools' route). */
  const serveTab = () => {
    state.canEdit = false;
    state.servers = TAB_PAYLOADS.servers.servers;
    state.responder = (method, url) => {
      const name = Object.keys(TAB_READS).find((k) => TAB_READS[k] === url);
      return name ? { status: 200, body: TAB_PAYLOADS[name] } : { status: 404, body: { error: "no such read " + url } };
    };
  };

  const scenarios = {
    /* T1: what a seat sees by what its session probe said: "readonly", "probefailed" (an HTTP error), "probedropped" (a transport
       failure) or "editor". The list carries a row without a server_id, which never gets an Edit button. */
    seat: async (kind) => {
      state.servers = [alpha, bravo, server(4, "Echo", { server_id: null })];
      state.byId = { 1: reads.alpha, 2: reads.bravo };
      if (kind === "readonly") state.canEdit = false;
      if (kind === "probefailed") state.sessionReply = () => ({ status: 500, body: { error: "session probe failed" } });
      if (kind === "probedropped") state.sessionReply = () => { throw new Error("connection refused"); };
      await mountPage("servers");
      const headRow = byTag(main, "table")[0].children[0].children[0];
      const edits = buttons(main, "Edit");
      return {
        editHeaders: headRow.children.filter((th) => (th.children[0] || th).textContent === "Edit").length,
        editButtons: edits.length,
        labels: edits.map((b) => b.attrs["aria-label"]),
        ids: edits.map((b) => b.attrs["data-server-id"]),
        rows: tableRows(),
        notice: all(main, (n) => n.className.split(/\s+/).includes("notice")).map(text),
        requests: requests.map((r) => r.method + " " + r.url),
        form: openForm() !== null,
      };
    },
    /* Edit on one row: "alpha" (Windows), "charlie" (SQL login), "bravo" (PostgreSQL, port 5433) or "bravo0" (PostgreSQL, port 0). */
    open: async (which) => {
      seed();
      if (which === "bravo0") state.byId[2] = { ...reads.bravo, port: 0 };
      await mountPage("servers");
      await clickEdit({ alpha: 1, bravo: 2, bravo0: 2, charlie: 3 }[which]);
      return { form: formSnapshot(), byIdGets: byIdGets(), formBoxChildren: box("form").children.length, requests: requests.length };
    },
    /* The by-id read held back: the box says it is loading, then the form replaces it. */
    loading: async () => {
      seed();
      await mountPage("servers");
      let release;
      state.byIdGate = new Promise((r) => { release = r; });
      const click = clickEdit(1);
      await settle();
      const during = { box: text(box("form")), form: openForm() !== null, strips: strips("form").map((s) => s.cls) };
      release();
      await click;
      await settle();
      return { during, form: formSnapshot() };
    },
    /* T8 and finding 9: the by-id read answers "404" (the server was removed), "403", "500" or "network". */
    byIdFails: async (kind) => {
      seed();
      await mountPage("servers");
      if (kind === "404") {
        state.byId = {};
        state.servers = [bravo, charlie];
      }
      if (kind === "403") state.byIdReply = () => ({ status: 403, body: { error: "This account has read-only access." } });
      if (kind === "500") state.byIdReply = () => ({ status: 500, body: { error: "admin server read failed (InvalidOperationException)" } });
      if (kind === "network") state.byIdReply = () => { throw new Error("connection refused"); };
      const listBefore = listGets();
      await clickEdit(1);
      return {
        notice: strips("notice"),
        form: openForm() !== null,
        formBoxChildren: box("form").children.length,
        listReads: listGets() - listBefore,
        rows: tableRows(),
        byIdGets: byIdGets(),
      };
    },
    /* T10: the 60 s poll runs renderAdmin again while the user types in an open form. "ok" lets the list read land; "listFails"
       makes it answer 500. The list read is held until the focus and the value have been looked at. */
    poll: async (kind) => {
      seed();
      await mountPage("servers");
      await clickEdit(1);
      const host = field("host");
      await typeInto(host, "sql-prod-01");
      const formBox = box("form");
      const removedBefore = removed.length;
      state.servers = [alpha, charlie];
      if (kind === "listFails") state.listReply = () => ({ status: 500, body: { error: "the list could not be read" } });
      let release;
      state.listGate = new Promise((r) => { release = r; });
      page.renderAdmin(main, "servers");
      await settle();
      const during = { active: activeElement === host, value: host.value, rows: tableRows() };
      release();
      await settle(30);
      return {
        during,
        activeIsHost: activeElement === host,
        sameHost: field("host") === host,
        hostValue: host.value,
        hostConnected: host.isConnected,
        sameFormBox: box("form") === formBox,
        ancestorsRemoved: removed.slice(removedBefore).filter((n) => n.contains(formBox)).length,
        formOpen: openForm() !== null,
        rows: tableRows(),
        tableArea: text(box("table")),
        tableAreaStrips: strips("table").map((s) => s.cls),
        notice: strips("notice").map((s) => s.text),
        listReads: listGets(),
      };
    },
    /* Finding 3 and the discard triggers: a password typed into Charlie's form, then the form goes away by "cancel", by Edit
       on "another" row or by a switch to the Routes "tab". The node typed into is kept, so it is read after it is detached. */
    closes: async (path) => {
      seed();
      await mountPage("servers");
      await clickEdit(3);
      const pw = field("password");
      await typeInto(pw, "SECRET-PW");
      const typed = pw.value;
      if (path === "cancel") await clickText(main, "Cancel");
      else if (path === "another") await clickEdit(1);
      else await mountPage("routes");
      await settle();
      const secret = (n) => n.value === "SECRET-PW" || n.textContent.includes("SECRET-PW") || Object.values(n.attrs).some((v) => String(v).includes("SECRET-PW"));
      const open = formSnapshot();
      return {
        typed,
        value: pw.value,
        connected: pw.isConnected,
        forms: byAttr(main, "data-edit-form").length,
        heading: open ? open.heading : null,
        secretNodes: all(main, secret).length,
      };
    },
    /* A by-id read that lands after its form was discarded (by Edit on "another" row, or a switch to the Routes "tab") opens nothing. */
    staleOpen: async (path) => {
      seed();
      await mountPage("servers");
      let release;
      state.byIdGate = new Promise((r) => { release = r; });
      const first = clickEdit(1);
      await settle();
      let second = Promise.resolve();
      if (path === "another") second = clickEdit(2);
      else await mountPage("routes");
      await settle();
      release();
      await Promise.all([first, second]);
      await settle();
      const open = formSnapshot();
      return { forms: byAttr(main, "data-edit-form").length, heading: open ? open.heading : null, byIdGets: byIdGets() };
    },
    /* The authentication choice and the boxes it shows. Alpha is a Windows server; the steps pick each mode in turn, typing a
       username under SQL. Then Charlie (a SQL login) shows when the password becomes required: a changed host. */
    authModes: async () => {
      seed();
      await mountPage("servers");
      await clickEdit(1);
      const snap = (step) => ({
        step,
        checked: byAttr(main, "data-field", "auth").filter((r) => r.checked).map((r) => r.attrs.value),
        username: rowInfo("username"),
        usernameValue: field("username").value,
        password: rowInfo("password"),
        note: rowInfo("managed-identity-note"),
      });
      const choose = async (word) => {
        const radio = byAttr(main, "data-field", "auth").find((r) => r.attrs.value === word);
        radio.checked = true;
        await fire(radio, "change");
      };
      const steps = [snap("opened")];
      await choose("SQL");
      await typeInto(field("username"), "sa-new");
      steps.push(snap("sql"));
      await choose("ServicePrincipal");
      steps.push(snap("serviceprincipal"));
      await choose("SQL");
      steps.push(snap("sql again"));
      await choose("ManagedIdentity");
      steps.push(snap("managed identity"));
      await choose("Windows");
      steps.push(snap("windows"));
      await clickEdit(3);
      const suffixes = [rowInfo("password").label];
      await typeInto(field("host"), "charlie-two");
      suffixes.push(rowInfo("password").label);
      await typeInto(field("host"), "charlie");
      suffixes.push(rowInfo("password").label);
      return { steps, suffixes };
    },
    /* D15: the stored encryption word, in any case or unknown ("none" is a stored null), and the options the form offers. */
    encrypt: async (word) => {
      seed();
      state.byId[1] = { ...reads.alpha, encrypt_mode: word === "none" ? null : word };
      await mountPage("servers");
      await clickEdit(1);
      const select = field("encrypt_mode");
      return { value: select.value, options: all(select, (n) => n.tag === "option").map((o) => o.attrs.value) };
    },
    /* Save, one case of SAVES at a time (T5, T7, T8's PATCH 404, T11, the answer table, and the status line of review finding 5).
       The PATCH is held at the service while "during" is taken, then released. `pw` is the password box typed into, read after
       the answer; `expired` is what the shell was told when the session was gone. */
    save: async (kind) => {
      const c = SAVES[kind];
      if (!c) throw new Error("unknown save case " + kind);
      seed();
      const expired = [];
      util.onSessionExpired((message, login) => expired.push({ message, login }));
      await mountPage("servers");
      await clickEdit(c.id);
      const pw = field("password");
      for (const [key, value] of Object.entries(c.edits)) await edit(key, value);
      if (c.typed) await typeInto(pw, SECRET);
      const typed = pw.value;
      let n = 0;
      state.responder = (method, url, sent) => c.answer(sent, ++n);
      if (c.down) state.networkDown = true;
      const during = await gatedSave();
      const after = view();
      const result = { typed, during, after };
      if (c.again) {
        await clickText(main, "Save");
        await settle(30);
        result.second = view();
      }
      result.pw = { value: pw.value, connected: pw.isConnected };
      result.secretNodes = secretNodes();
      result.expired = expired;
      return result;
    },
    /* T9 and review finding 4: a save held at the service, and what the page lets happen meanwhile. "double": a second click on the
       disabled Save and a click forced past it send no second PATCH; "429": the same, and the service then answers 429. "edit": Edit on
       another row (clicked, and forced past the disabled state) opens nothing; after the answer the notice shows the outcome and the
       next row's form saves. "tab" and "hash": the form is discarded mid-save (the Routes tab; another page) and the late answer
       still sets the notice, which shows when the Servers tab is back, and the list is read again. */
    midsave: async (how) => {
      seed();
      await mountPage("servers");
      await clickEdit(1);
      await edit("display_name", "Alpha Two");
      state.responder = (method) => {
        if (method !== "PATCH") return { status: 200, body: TAB_PAYLOADS.routes };
        if (how === "429") return { status: 429, body: { error: "Another server change is in progress. Try again in a moment." } };
        renamed(1, "Alpha Two");
        return updated("Alpha Two");
      };
      let release;
      state.gate = new Promise((r) => { release = r; });
      const first = clickText(main, "Save");
      await settle();
      const during = view();
      if (how === "double" || how === "429") {
        await clickText(main, "Save");
        await forceClick(buttons(openForm(), "Save")[0]);
      }
      if (how === "edit") {
        await clickEdit(2);
        await forceClick(byAttr(main, "data-server-id", 2).find((b) => b.tag === "button"));
      }
      if (how === "tab") await mountPage("routes");
      if (how === "hash") await leaveHash("#/fleet");
      const mid = view();
      release();
      await first;
      await settle(30);
      state.gate = null;
      const result = { during, mid, late: view() };
      if (how === "tab") {
        await mountPage("servers");
        result.back = view();
      }
      if (how === "edit") {
        await clickEdit(3);
        await edit("display_name", "Charlie Two");
        state.responder = () => updated("Charlie Two");
        await clickText(main, "Save");
        await settle(30);
        result.next = view();
      }
      if (how === "429") {
        await settle(30);
        result.settled = view();
      }
      return result;
    },
    /* The hashchange discard (D6): a password typed into Charlie's form and a changed host, then the hash moves to `hash`. The page
       is painted twice first, so the listener count shows it is registered once. `kept` is whether the same form is still open. */
    hash: async (h) => {
      seed();
      await mountPage("servers");
      await clickEdit(3);
      const form = openForm();
      const pw = field("password");
      await typeInto(pw, SECRET);
      await typeInto(field("host"), "charlie-two");
      await mountPage("servers");
      const listeners = (windowHandlers.hashchange || []).length;
      await leaveHash(h);
      return {
        listeners,
        kept: openForm() === form && form.isConnected,
        host: field("host") ? field("host").value : null,
        value: pw.value,
        connected: pw.isConnected,
        forms: byAttr(main, "data-edit-form").length,
        formBoxChildren: box("form").children.length,
        secretNodes: secretNodes(),
      };
    },
    /* A by-id read still in flight when the hash leaves the Servers tab: the form it would have filled never opens. */
    hashOpen: async (h) => {
      seed();
      await mountPage("servers");
      let release;
      state.byIdGate = new Promise((r) => { release = r; });
      const click = clickEdit(1);
      await settle();
      await leaveHash(h);
      release();
      await click;
      await settle();
      return { forms: byAttr(main, "data-edit-form").length, formBoxChildren: box("form").children.length };
    },
    /* T4: a stale edit on Charlie's form. The user changes the host and the display name and types the password; the first save is
       answered 409 conflict with what another edit left (a new host, its own display name, a database, a newer modified_at), or, for
       "none", with nothing of this form changed (another setting did). `how` is what the user does next: "reapply" and "reload" press
       that button; "reloadnone" presses Reload and then Save at once; "again" presses Save without choosing; "none" presses Reapply on
       the answer with nothing changed. Requests are counted around the press; the next Save is the user's own. */
    conflict: async (how) => {
      seed();
      const none = how === "none";
      const other = none ? { ...reads.charlie, modified_at: TOKEN2 } : { ...reads.charlie, host: "charlie-new", display_name: "Charlie Elsewhere", database: "Orders", modified_at: TOKEN2 };
      await mountPage("servers");
      await clickEdit(3);
      if (!none) await edit("host", "charlie-two");
      await edit("display_name", "Charlie Two");
      const pw = field("password");
      await typeInto(pw, SECRET);
      let n = 0;
      state.responder = () => (++n === 1 || (how === "again" && n === 2) ? conflictAnswer(other) : updated("Charlie Two"));
      const take = () => ({ view: view(), form: formSnapshot(), requests: requests.length, secretNodes: secretNodes(), otherNodes: leaks("OTHER-PW") });
      const save = async (name) => {
        await clickText(main, "Save");
        await settle(30);
        result[name] = take();
      };
      const result = {};
      await save("stale");
      result.stale.pw = { value: pw.value, connected: pw.isConnected };
      const before = requests.length;
      if (how === "reapply" || none) await clickText(main, "Reapply my changes");
      if (how === "reload" || how === "reloadnone") await clickText(main, "Reload current values");
      result.pressed = { ...take(), sent: requests.length - before, oldPw: { value: pw.value, connected: pw.isConnected } };
      if (how === "reapply") {
        await save("refused");
        await typeInto(field("password"), "OTHER-PW");
        await save("saved");
      } else if (how === "reload") {
        await edit("monthly_cost_usd", "5");
        await save("saved");
      } else if (how === "again") {
        await typeInto(field("password"), "OTHER-PW");
        await save("saved");
      } else await save("saved");
      return result;
    },
    /* T6 and the password's whole life: "SECRET-PW" typed into Charlie's form, then it leaves by `path`: "cancel"; "saved" (the service
       updates it); "badrequest", "echo" (a 400 whose message repeats the password) and "limited" (429); "conflict" (a 409 whose
       current values carry the password as a display name, so the panel line must show [redacted]); "reapply" and "reload" after
       a plain 409; "hash" (the window goes to another page); "listexpired" (the next list read answers 401, so the shell takes
       the page over). The password box typed into is kept, so it is read after it was detached. */
    secret: async (path) => {
      seed();
      await mountPage("servers");
      await clickEdit(3);
      const pw = field("password");
      await typeInto(pw, SECRET);
      const typed = pw.value;
      const stale = { ...reads.charlie, host: "charlie-new", modified_at: TOKEN2 };
      const answers = {
        saved: () => updated("Charlie"),
        badrequest: () => ({ status: 400, body: { error: "The host 'charlie two' is not valid." } }),
        echo: () => ({ status: 400, body: { error: "The host 'charlie two' is not valid: SECRET-PW is not allowed here." } }),
        limited: () => ({ status: 429, body: { error: "Another server change is in progress. Try again in a moment." } }),
        conflict: () => conflictAnswer({ ...stale, display_name: SECRET }),
        reapply: () => conflictAnswer(stale),
        reload: () => conflictAnswer(stale),
      };
      if (path in answers) {
        await edit("host", "charlie-two");
        state.responder = answers[path];
        await clickText(main, "Save");
        await settle(30);
        if (path === "reapply") await clickText(main, "Reapply my changes");
        if (path === "reload") await clickText(main, "Reload current values");
      } else if (path === "cancel") await clickText(main, "Cancel");
      else if (path === "hash") await leaveHash("#/fleet");
      else if (path === "listexpired") {
        state.listReply = () => ({ status: 401, body: { error: "Session expired" } });
        page.renderAdmin(main, "servers");
        await settle(30);
      } else throw new Error("unknown secret path " + path);
      const next = field("password");
      return {
        typed,
        value: pw.value,
        connected: pw.isConnected,
        nextValue: next ? next.value : null,
        forms: byAttr(main, "data-edit-form").length,
        secretNodes: secretNodes(),
        view: view(),
      };
    },
    /* T2, T3 and T13, the flow halves: one edit driven through the real form for the by-id read given on stdin
       ({"read": {...}, "edits": {"host": "sql-b"}, "password": "typed"}). The edits are made in the order given, the authentication
       first (the username box belongs to a mode); the password is typed into its box even when the page hides it. The service answers
       "updated". Prints the form as it opened, the label of the password row once the edits were made, and the page after Save. */
    flow: async () => {
      const request = JSON.parse(await readStdin());
      const read = request.read;
      state.servers = [server(read.server_id, read.display_name, { engine: read.engine })];
      state.byId = { [read.server_id]: read };
      state.responder = () => updated(read.display_name);
      await mountPage("servers");
      await clickEdit(read.server_id);
      const opened = formSnapshot();
      const entries = Object.entries(request.edits || {});
      for (const [key, value] of entries.filter(([k]) => k === "auth")) await chooseAuth(value);
      for (const [key, value] of entries.filter(([k]) => k !== "auth")) await edit(key, value);
      if (request.password) await typeInto(field("password"), request.password);
      const label = rowInfo("password").label;
      await clickText(main, "Save");
      await settle(30);
      return { opened, label, after: view(), patches: patches() };
    },
    /* Finding 11: the attributes of the username and password boxes of a SQL Server form and of a PostgreSQL form, and whether either
       sits inside a form element (or the page has one at all). */
    inputs: async () => {
      seed();
      await mountPage("servers");
      const out = {};
      for (const [name, id] of [["sql", 3], ["postgres", 2]]) {
        await clickEdit(id);
        const describe = (key) => {
          const node = field(key);
          let inForm = false;
          for (let a = node.parent; a; a = a.parent) if (a.tag === "form") inForm = true;
          return { type: node.attrs.type, attrs: { ...node.attrs }, inForm };
        };
        out[name] = { username: describe("username"), password: describe("password"), formElements: byTag(main, "form").length };
      }
      return out;
    },
    /* The ported tests of the old vm harness: one tab drawn through the real modules, a read-only seat, every piece of text it drew. */
    tab: async (name) => {
      serveTab();
      await mountPage(name);
      const table = byTag(main, "table")[0];
      const trs = table ? table.children[1].children : [];
      return {
        texts: textsUnder(main),
        reads: requests.filter((r) => r.url !== "/api/session").map((r) => r.method + " " + r.url),
        rowClasses: trs.map((tr) => tr.className),
        cells: trs.map((tr) => tr.children.map(text)),
        addedFormatted: util.localTime("2026-01-02T00:00:00.0000000"),
        utcOffsetMinutes: new Date(2026, 0, 1).getTimezoneOffset(),
      };
    },
    /* The 60 s poll: the tab on screen is rendered again and the page is looked at BEFORE the read lands. */
    repaint: async (name) => {
      serveTab();
      await mountPage(name);
      const before = main.children[2];
      const row = name === "servers" ? "Alpha" : "cpu.threshold_percent";
      let release;
      if (name === "servers") state.listGate = new Promise((r) => { release = r; });
      else state.gate = new Promise((r) => { release = r; });
      page.renderAdmin(main, name);
      await settle();
      const during = textsUnder(main.children[2]);
      const result = {
        sameBody: main.children[2] === before,
        loadingShown: during.some((t) => t.startsWith("Loading")),
        rowsKept: during.length > 1,
        rowShown: during.includes(row),
      };
      release();
      await settle(30);
      result.rowAfter = textsUnder(main).includes(row);
      return result;
    },
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
        utcOffsetMinutes: new Date(2026, 0, 1).getTimezoneOffset(),
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
    /* One call of an admin.js export, {"fn": "buildEditBody", "args": [...]} on stdin, printed as {"result": ...}; or
       {"calls": [{"fn": ..., "args": [...]}, ...]} for a table of calls in one process, printed as {"results": [...]}.
       An export that is not a function (EDIT_FIELDS) is returned as it is. */
    pure: async () => {
      const request = JSON.parse(await readStdin());
      const call = ({ fn, args = [] }) => {
        if (!(fn in page)) throw new Error("admin.js exports no " + fn);
        return typeof page[fn] === "function" ? page[fn](...args) : page[fn];
      };
      return Array.isArray(request.calls) ? { results: request.calls.map(call) } : { result: call(request) };
    },
  };
  if (!(scenario in scenarios)) throw new Error("unknown scenario " + scenario);
  output = await scenarios[scenario](scenarioParam);
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
const nonFinite = (k, v) => (typeof v === "number" && !Number.isFinite(v) ? { $number: String(v) } : v);
process.stdout.write(JSON.stringify(output, nonFinite) + "\n", () => process.exit(0));
