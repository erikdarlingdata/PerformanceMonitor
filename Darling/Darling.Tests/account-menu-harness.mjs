/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* Runs the shipped account-menu.js (with the real util.js) under Node against a stand-in /api/me, and prints the result of a
   scenario as one line of JSON (#8737). AccountMenuWebTests starts it as
       node account-menu-harness.mjs <path to wwwroot/js> <scenario> <json>
   <json> is the /api/me answer (an object), or a number for an HTTP status with no body. The DOM is a small tree that records every
   text node, every textContent and every innerHTML assignment, so a test can tell text from markup. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [jsDir, scenario, rawJson] = process.argv.slice(-3);
const input = JSON.parse(rawJson);

const noop = () => {};
const texts = [];
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
    const set = new Set();
    this.classList = { add: (c) => set.add(c), remove: (c) => set.delete(c), toggle: noop, contains: (c) => set.has(c) };
  }
  set textContent(v) { texts.push(String(v)); this._text = String(v); }
  get textContent() { return this._text; }
  set innerHTML(v) { innerHtmlSets.push(String(v)); }
  appendChild(c) { this.childNodes.push(c); this.firstChild = this.childNodes[0]; return c; }
  removeChild(c) { this.childNodes.splice(this.childNodes.indexOf(c), 1); this.firstChild = this.childNodes[0] || null; return c; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  removeAttribute(k) { delete this.attrs[k]; }
  addEventListener(t, f) { (this.listeners[t] = this.listeners[t] || []).push(f); }
  removeEventListener() {}
  contains() { return false; }
  focus() {}
  querySelector() { return null; }
  querySelectorAll() { return []; }
};
globalThis.document = {
  createElement: (tag) => new Node(tag),
  createTextNode: (t) => { texts.push(String(t)); return new Node("#text", String(t)); },
  getElementById: () => null, querySelector: () => null, querySelectorAll: () => [],
  addEventListener: noop, removeEventListener: noop, body: new Node("body"), documentElement: new Node("html"), cookie: "", hidden: false,
};
globalThis.window = globalThis;
globalThis.location = { hash: "", pathname: "/", search: "", href: "http://localhost/", origin: "http://localhost" };
const store = { getItem: () => null, setItem: noop, removeItem: noop };
globalThis.localStorage = store;
globalThis.sessionStorage = store;
globalThis.addEventListener = noop;
globalThis.removeEventListener = noop;

let fetched = [];
globalThis.fetch = async (url) => {
  fetched.push(url);
  if (typeof input === "number") return { ok: false, status: input, text: async () => "" };
  return { ok: true, status: 200, text: async () => JSON.stringify(input) };
};

/* Everything under a node that matches, depth first. */
const find = (n, pred, acc = []) => {
  if (pred(n)) acc.push(n);
  for (const c of n.childNodes) find(c, pred, acc);
  return acc;
};
const flat = (n) => (n.tag === "#text" ? n.text : n._text + n.childNodes.map(flat).join(""));

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "account-menu-"));
const out = {};
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const menu = await import(pathToFileURL(path.join(scratch, "account-menu.js")).href);

  if (scenario === "line") {
    out.line = menu.accountLine(input);
  } else if (scenario === "details") {
    out.details = menu.accountDetails(input);
    out.localExpiry = input.expires_at ? new Date(input.expires_at).toLocaleString() : null;
  } else if (scenario === "init") {
    const brand = new Node("div");
    brand.hidden = true;
    const top = new Node("span");
    top.hidden = true;
    await menu.initAccountMenu(brand, top);
    out.fetched = fetched;
    out.brandHidden = brand.hidden;
    out.topbarHidden = top.hidden;
    out.texts = texts;
    out.innerHtmlSets = innerHtmlSets;
    if (!brand.hidden) {
      const button = find(brand, (n) => n.tag === "button")[0];
      out.buttonText = flat(button);
      out.buttonAttrs = button.attrs;
      /* Open the popover as a click would, then read what it holds. */
      button.listeners.click[0]();
      const pop = find(brand, (n) => n.attrs.role === "menu")[0];
      out.popupRows = find(pop, (n) => n.className === "account-row").map((r) => r.childNodes.map(flat));
      out.menuItems = find(pop, (n) => n.attrs.role === "menuitem").map((n) => ({ tag: n.tag, href: n.attrs.href, text: flat(n) }));
      out.expandedAfterOpen = button.attrs["aria-expanded"];
      /* Escape closes it again. */
      find(brand, (n) => n.className === "account-control")[0].listeners.keydown[0]({ key: "Escape", preventDefault: noop });
      out.expandedAfterEscape = button.attrs["aria-expanded"];
      out.popupAfterEscape = find(brand, (n) => n.attrs.role === "menu").length;
      out.topbarHasControl = find(top, (n) => n.tag === "button").length;
    }
  } else {
    throw new Error("unknown scenario " + scenario);
  }
  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
process.exit(0);
