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
let focused = null;
const docListeners = {};
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
  appendChild(c) { c.parent = this; this.childNodes.push(c); this.firstChild = this.childNodes[0]; return c; }
  removeChild(c) { c.parent = null; this.childNodes.splice(this.childNodes.indexOf(c), 1); this.firstChild = this.childNodes[0] || null; return c; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  removeAttribute(k) { delete this.attrs[k]; }
  addEventListener(t, f) { (this.listeners[t] = this.listeners[t] || []).push(f); }
  removeEventListener() {}
  contains(n) { for (let x = n; x; x = x.parent) if (x === this) return true; return false; }
  focus() { focused = this; }
  querySelector() { return null; }
  querySelectorAll() { return []; }
};
globalThis.document = {
  createElement: (tag) => new Node(tag),
  createTextNode: (t) => { texts.push(String(t)); return new Node("#text", String(t)); },
  getElementById: () => null, querySelector: () => null, querySelectorAll: () => [],
  addEventListener: (t, f) => { (docListeners[t] = docListeners[t] || []).push(f); },
  removeEventListener: (t, f) => { docListeners[t] = (docListeners[t] || []).filter((x) => x !== f); },
  get activeElement() { return focused; },
  body: new Node("body"), documentElement: new Node("html"), cookie: "", hidden: false,
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
      out.buttonText = flat(button.childNodes[0]);
      out.caret = find(button, (n) => n.className === "account-caret").map((n) => ({ hidden: n.attrs["aria-hidden"], text: flat(n) }));
      out.buttonAttrs = button.attrs;
      /* Open the popover as a click would, then read what it holds. */
      button.listeners.click[0]();
      const pop = find(brand, (n) => n.className === "account-pop")[0];
      out.popupRows = find(pop, (n) => n.className === "account-row").map((r) => r.childNodes.map(flat));
      out.panelId = pop.attrs.id;
      out.controls = button.attrs["aria-controls"];
      out.rolesUsed = find(brand, (n) => "role" in n.attrs).length;
      out.hasPopupAttr = "aria-haspopup" in button.attrs;
      out.menuItems = find(pop, (n) => n.tag === "a").map((n) => ({ tag: n.tag, href: n.attrs.href, text: flat(n) }));
      out.expandedAfterOpen = button.attrs["aria-expanded"];
      /* Escape closes it again. */
      (docListeners.keydown || []).slice().forEach((f) => f({ key: "Escape", preventDefault: noop }));
      out.expandedAfterEscape = button.attrs["aria-expanded"];
      out.popupAfterEscape = find(brand, (n) => n.className === "account-pop").length;
      out.topbarHasControl = find(top, (n) => n.tag === "button").length;
    }
  } else if (scenario === "interact") {
    const brand = new Node("div");
    const top = new Node("span");
    await menu.initAccountMenu(brand, top);
    const button = find(brand, (n) => n.tag === "button")[0];
    const control = find(brand, (n) => n.className === "account-control")[0];
    const panels = () => find(brand, (n) => n.className === "account-pop").length;
    const clickDoc = (target) => (docListeners.click || []).slice().forEach((f) => f({ target }));
    const settle = () => new Promise((r) => setTimeout(r, 20));
    button.listeners.click[0]();
    await settle();
    out.openedPanels = panels();
    clickDoc(find(brand, (n) => n.className === "account-pop")[0].childNodes[0]);
    out.panelsAfterClickInside = panels();
    clickDoc(new Node("div"));
    out.panelsAfterClickOutside = panels();
    out.expandedAfterClickOutside = button.attrs["aria-expanded"];
    out.outsideListenersLeft = (docListeners.click || []).length;
    button.listeners.click[0]();
    await settle();
    focused = null;
    const pressEscape = () => (docListeners.keydown || []).slice().forEach((f) => f({ key: "Escape", preventDefault: noop }));
    pressEscape();
    out.panelsAfterEscape = panels();
    out.focusAfterEscape = focused === button;
    out.keydownListenersLeft = (docListeners.keydown || []).length;
    /* Opened by a click that never focused the button: Escape on the document still closes it. */
    button.listeners.click[0]();
    await settle();
    focused = null;
    pressEscape();
    out.panelsAfterEscapeWithoutFocus = panels();
    button.listeners.click[0]();
    await settle();
    focused = null;
    const next = new Node("a");
    const focusout = (relatedTarget) => control.listeners.focusout[0]({ relatedTarget });
    focusout(find(brand, (n) => n.className === "account-pop")[0].childNodes[0]);
    out.panelsAfterFocusMovesInside = panels();
    focusout(null);
    out.panelsAfterFocusLost = panels();
    focused = next;
    focusout(next);
    out.panelsAfterTab = panels();
    out.focusAfterTab = focused === next;
    out.topAndBrandIds = [find(top, (n) => n.tag === "button")[0].attrs["aria-controls"], button.attrs["aria-controls"]];
  } else {
    throw new Error("unknown scenario " + scenario);
  }
  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
process.exit(0);
