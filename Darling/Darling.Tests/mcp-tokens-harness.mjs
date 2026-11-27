/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* Runs the shipped My MCP tokens page (wwwroot/js/pages/mcp-tokens.js, with the real util.js) under Node against stand-in
   /api/mcp-tokens routes, and prints what a scenario measured as one line of JSON (#8737). McpTokensWebTests starts it as
       node mcp-tokens-harness.mjs <path to wwwroot/js> <scenario> <json>
   <json> is { me, own, all, create, revoke, fill, times }: the /api/me answer, and for each route { status, body } (a route left out
   answers 200 with {}). The DOM is a small tree that records every innerHTML assignment, so a test can tell text from markup.
   Scenarios: render (draw the page), create (fill the form with json.fill and click Create, then draw the page again),
   leave (create, then the address changes), revoke (click the first Revoke; json.confirm false declines the confirmation). */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [jsDir, scenario, rawJson] = process.argv.slice(-3);
const cfg = JSON.parse(rawJson);

const noop = () => {};
const innerHtmlSets = [];
globalThis.Node = class Node {
  constructor(tag, text) {
    this.tag = tag;
    this.attrs = {};
    this.childNodes = [];
    this.firstChild = null;
    this.style = {};
    this.dataset = {};
    this.hidden = false;
    this.className = "";
    this.listeners = {};
    this.text = text;
    this._text = "";
    this.classList = { add: noop, remove: noop, toggle: noop, contains: () => false };
  }
  set textContent(v) { this._text = String(v); }
  get textContent() { return this._text; }
  set innerHTML(v) { innerHtmlSets.push(String(v)); }
  appendChild(c) { c.parent = this; this.childNodes.push(c); this.firstChild = this.childNodes[0]; return c; }
  removeChild(c) { c.parent = null; this.childNodes.splice(this.childNodes.indexOf(c), 1); this.firstChild = this.childNodes[0] || null; return c; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  removeAttribute(k) { delete this.attrs[k]; }
  addEventListener(t, f) { (this.listeners[t] = this.listeners[t] || []).push(f); }
  removeEventListener() {}
  contains(n) { for (let x = n; x; x = x.parent) if (x === this) return true; return false; }
  focus() {}
  querySelector() { return null; }
  querySelectorAll() { return []; }
};
globalThis.document = {
  createElement: (tag) => new Node(tag),
  createTextNode: (t) => new Node("#text", String(t)),
  getElementById: () => null, querySelector: () => null, querySelectorAll: () => [],
  addEventListener: noop, removeEventListener: noop,
  body: new Node("body"), documentElement: new Node("html"), cookie: "", hidden: false,
};
globalThis.window = globalThis;
globalThis.location = { hash: "", pathname: "/", search: "", href: "http://localhost/", origin: "http://localhost" };
const store = { getItem: () => null, setItem: noop, removeItem: noop };
globalThis.localStorage = store;
globalThis.sessionStorage = store;
const windowListeners = {};
globalThis.addEventListener = (t, f) => { (windowListeners[t] = windowListeners[t] || []).push(f); };
const confirms = [];
globalThis.confirm = (m) => { confirms.push(m); return cfg.confirm !== false; };
globalThis.removeEventListener = noop;
let copied = null;
Object.defineProperty(globalThis, "navigator", { value: { clipboard: { writeText: async (t) => { copied = t; } } }, configurable: true });

const requests = [];
globalThis.fetch = async (url, init) => {
  const method = ((init && init.method) || "GET").toUpperCase();
  const u = String(url);
  requests.push({
    method,
    url: u,
    contentType: init && init.headers ? init.headers["Content-Type"] || null : null,
    hasBody: !!(init && init.body),
    body: init && init.body ? JSON.parse(init.body) : null,
  });
  let route;
  if (method === "GET" && u === "/api/mcp-tokens") route = cfg.own;
  else if (method === "GET" && u === "/api/mcp-tokens?all=1") route = cfg.all;
  else if (method === "POST" && u === "/api/mcp-tokens") route = cfg.create;
  else if (method === "POST" && /^\/api\/mcp-tokens\/[^/]+\/revoke$/.test(u)) route = cfg.revoke;
  route = route || { status: 200, body: {} };
  const text = route.body === undefined ? "" : JSON.stringify(route.body);
  return { ok: route.status >= 200 && route.status < 300, status: route.status, url: u, text: async () => text, headers: { get: () => null } };
};

const find = (n, pred, acc = []) => {
  if (pred(n)) acc.push(n);
  for (const c of n.childNodes) find(c, pred, acc);
  return acc;
};
const flat = (n) => (n.tag === "#text" ? n.text : n._text + n.childNodes.map(flat).join(""));
const byClass = (n, c) => find(n, (x) => x.className.split(/\s+/).includes(c));

/* What the page shows, as plain values. */
const snap = (host) => ({
  h3s: find(host, (n) => n.tag === "h3").map(flat),
  strips: find(host, (n) => n.className.startsWith("strip ")).map(flat),
  tables: byClass(host, "mcp-tokens").map((t) => ({
    head: find(t, (n) => n.tag === "th").map(flat),
    rows: find(t, (n) => n.tag === "tr" && n.attrs["data-token"] !== undefined).map((r) => ({
      id: r.attrs["data-token"],
      cells: r.childNodes.map(flat),
      revoke: find(r, (n) => n.tag === "button").length,
    })),
  })),
  roleOptions: find(host, (n) => n.tag === "option").map((o) => o.attrs.value),
  hasForm: byClass(host, "mcp-token-form").length,
  valuePanel: byClass(host, "token-value-panel").map((p) => ({
    name: flat(byClass(p, "token-value-name")[0]),
    note: flat(byClass(p, "token-value-note")[0]),
    value: flat(byClass(p, "token-value")[0]),
  })),
  maxAttr: find(host, (n) => n.tag === "input" && n.attrs.type === "number").map((n) => n.attrs.max),
});

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "mcp-tokens-"));
const out = {};
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const page = await import(pathToFileURL(path.join(scratch, "pages", "mcp-tokens.js")).href);
  const host = new Node("div");
  await page.renderMcpTokens(host, cfg.me);
  out.first = snap(host);
  out.requestsOnLoad = requests.slice();
  out.local = Object.fromEntries((cfg.times || []).map((t) => [t, new Date(t).toLocaleString()]));

  if (scenario === "create" || scenario === "leave") {
    const [name, days, role] = find(host, (n) => n.tag === "input" || n.tag === "select");
    name.value = cfg.fill.name;
    days.value = cfg.fill.lifetime_days;
    role.value = cfg.fill.role;
    const before = requests.length;
    await find(host, (n) => n.tag === "button" && flat(n) === "Create token")[0].listeners.click[0]();
    out.afterCreate = snap(host);
    out.createRequests = requests.slice(before);
    out.nameAfter = name.value;
    const copy = find(host, (n) => n.tag === "button" && flat(n) === "Copy")[0];
    if (copy) {
      await copy.listeners.click[0]();
      out.copied = copied;
      out.copyText = flat(copy);
    }
    if (scenario === "leave") {
      (windowListeners.hashchange || []).forEach((f) => f({}));
      out.afterLeave = snap(host);
      out.copyButtonsAfterLeave = find(host, (n) => n.tag === "button" && flat(n) === "Copy").length;
    }
    const again = new Node("div");
    await page.renderMcpTokens(again, cfg.me);
    out.afterReload = snap(again);
  } else if (scenario === "revoke") {
    const before = requests.length;
    await find(host, (n) => n.tag === "button" && flat(n) === "Revoke")[0].listeners.click[0]();
    out.revokeRequests = requests.slice(before);
    out.confirms = confirms;
    out.afterRevoke = snap(host);
  } else if (scenario !== "render") {
    throw new Error("unknown scenario " + scenario);
  }
  out.innerHtmlSets = innerHtmlSets;
  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
process.exit(0);
