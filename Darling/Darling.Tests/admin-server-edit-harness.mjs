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
