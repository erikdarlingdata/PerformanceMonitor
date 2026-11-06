/* Runs the shipped Alert Rules list (wwwroot/js/pages/alert-rules.js) under Node against a recording fetch and a fake DOM, and prints
   as one line of JSON what the 60 s poll costs and does to a list the reader is working on (#7927): the requests the first paint
   makes, the requests a poll over an unchanged list makes, whether an open "New from template" menu survives the poll, which rules a
   poll reads again when one rule's version changed, and a poll whose list read fails. AlertRulesPollBehaviourTests starts it as
       node alert-rules-poll-harness.mjs <path to wwwroot/js> */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[2];

class FakeNode {
  constructor(tag, text) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.style = {};
    this.className = "";
    this.value = "";
    this.open = false;
    this.hidden = false;
    this.disabled = false;
    this.handlers = {};
    this.text = text == null ? null : String(text);
  }
  get firstChild() { return this.children[0] || null; }
  appendChild(child) {
    if (child.parent) child.parent.removeChild(child);
    child.parent = this;
    this.children.push(child);
    return child;
  }
  removeChild(child) {
    const i = this.children.indexOf(child);
    if (i >= 0) this.children.splice(i, 1);
    child.parent = null;
    return child;
  }
  insertBefore(next, ref) {
    if (next.parent) next.parent.removeChild(next);
    next.parent = this;
    const i = ref ? this.children.indexOf(ref) : -1;
    if (i >= 0) this.children.splice(i, 0, next);
    else this.children.push(next);
    return next;
  }
  replaceChild(next, old) {
    const i = this.children.indexOf(old);
    if (i >= 0) {
      if (next.parent) next.parent.removeChild(next);
      next.parent = this;
      this.children[i] = next;
      old.parent = null;
    }
    return old;
  }
  get isConnected() {
    let n = this;
    while (n.parent) n = n.parent;
    return n.isRoot === true;
  }
  setAttribute(name, value) { this.attrs[name] = String(value); }
  getAttribute(name) { return name in this.attrs ? this.attrs[name] : null; }
  addEventListener(type, fn) { (this.handlers[type] ||= []).push(fn); }
  removeEventListener(type, fn) { this.handlers[type] = (this.handlers[type] || []).filter((f) => f !== fn); }
  closest() { return null; }
  focus() { globalThis.document.activeElement = this; }
  set textContent(value) { this.children = []; this.text = String(value); }
  get textContent() { return (this.text || "") + this.children.map((c) => c.textContent).join(""); }
}

globalThis.Node = FakeNode;
globalThis.document = {
  activeElement: null,
  documentElement: { offsetHeight: 0 },
  createElement: (tag) => new FakeNode(tag),
  createTextNode: (text) => new FakeNode("#text", text),
  querySelectorAll: () => [],
};
globalThis.window = globalThis;
globalThis.scrollY = 0;
globalThis.scrollTo = () => {};
globalThis.location = { hash: "#/alert-rules", pathname: "/", search: "", href: "http://localhost/#/alert-rules", origin: "http://localhost" };
const stored = {};
const storage = {
  getItem: (k) => (Object.prototype.hasOwnProperty.call(stored, k) ? stored[k] : null),
  setItem: (k, v) => { stored[k] = String(v); },
  removeItem: (k) => { delete stored[k]; },
};
globalThis.localStorage = storage;
globalThis.sessionStorage = storage;

const all = (node, pred, out = []) => {
  if (pred(node)) out.push(node);
  for (const c of node.children) all(c, pred, out);
  return out;
};
const withClass = (node, cls) => all(node, (n) => (" " + n.className + " ").includes(" " + cls + " "));
const settle = () => new Promise((r) => setTimeout(r, 20));

/* The list payload carries the summary (name, enabled, version, last fired) but not the definition; the per-rule read carries the
   definition the metric, condition and scope chips are described from. */
const rule = (id, name, version, o = {}) => ({ id, name, enabled: true, version, description: null, updated_at: "2026-11-06T10:00:00Z", updated_by: "alice", last_fired: null, expires_at_utc: null, expired: false, ...o });
let list = [rule(1, "CPU hot", 1), rule(2, "PLE low", 1), rule(3, "Blocked sessions", 1)];
const definitions = {
  1: { metric: { measure: "cpu_utilization", aggregate: "max" }, predicate: { op: "ge", warnThreshold: 80, criticalThreshold: 95 }, scope: { mode: "all" } },
  2: { metric: { ratio: "page_life_expectancy" }, predicate: { op: "lt", warnThreshold: 300 }, scope: { mode: "servers", servers: ["a", "b", "c"] } },
  3: { metric: { measure: "blocked_sessions", aggregate: "max" }, predicate: { op: "outside", lowerBound: 10, upperBound: 100 }, scope: { mode: "servers", servers: ["a"] } },
};
const templates = { templates: [{ key: "cpu", name: "High CPU", description: "CPU over 80% for five minutes" }] };
let listFails = false;
const fetches = [];
let onListRead = null;
globalThis.fetch = async (url) => {
  const u = String(url).split("?")[0];
  fetches.push(u);
  const reply = (body, status = 200) => ({ ok: status < 400, status, text: async () => JSON.stringify(body) });
  if (u === "/api/session") return reply({ can_edit: true, can_own: true });
  if (u === "/api/alert-templates") return reply(templates);
  if (u === "/api/alerts") {
    if (onListRead) onListRead();
    return listFails ? reply({ error: "the store is down" }, 500) : reply(list);
  }
  const m = /^\/api\/alerts\/(\d+)$/.exec(u);
  if (m) {
    const r = list.find((x) => String(x.id) === m[1]);
    return reply({ ...r, definition: definitions[m[1]] });
  }
  return reply([]);
};
const since = (from) => fetches.slice(from).sort();
const chipsOf = (main) => withClass(main, "view-card").map((c) => withClass(c, "vc-chip").map((n) => n.textContent).join(" | "));
const namesOf = (main) => withClass(main, "vc-name-text").map((n) => n.textContent);
const templateMenu = (main) => withClass(main, "template-menu")[0] || null;

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "alert-rules-poll-"));
const out = {};
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const page = await import(pathToFileURL(path.join(scratch, "pages", "alert-rules.js")).href);
  const main = new FakeNode("main");
  main.isRoot = true;

  /* The first paint: the session, the list and the templates, then one read per rule for the chips. */
  await page.renderAlertRuleList(main);
  await settle();
  out.first = { names: namesOf(main), chips: chipsOf(main), requests: since(0), templateItems: withClass(main, "template-item").map((n) => n.textContent) };

  /* The reader opens the template menu; the 60 s poll runs over an unchanged list. */
  const menuBefore = templateMenu(main);
  menuBefore.open = true;
  let loadingDuringPoll = null;
  onListRead = () => { loadingDuringPoll = main.textContent.includes("Loading alert rules"); };
  let from = fetches.length;
  await page.renderAlertRuleList(main, { poll: true });
  await settle();
  onListRead = null;
  out.unchangedPoll = {
    loadingDuringPoll,
    requests: since(from),
    menuKept: templateMenu(main) === menuBefore && templateMenu(main).open === true,
    names: namesOf(main),
    chips: chipsOf(main),
    refreshError: withClass(main, "refresh-error").length,
  };

  /* A poll over a list where one rule was renamed and another has a new version with a new condition. */
  list = [rule(1, "CPU hot", 1), rule(2, "PLE low, renamed", 1), rule(3, "Blocked sessions", 2)];
  definitions[3] = { metric: { measure: "blocked_sessions", aggregate: "max" }, predicate: { op: "gt", warnThreshold: 5 }, scope: { mode: "all" } };
  from = fetches.length;
  await page.renderAlertRuleList(main, { poll: true });
  await settle();
  out.changedPoll = { requests: since(from), names: namesOf(main), chips: chipsOf(main), menuKept: templateMenu(main) === menuBefore && templateMenu(main).open === true };

  /* A poll whose list read fails: the list on screen stays, with the failure noted under the heading. */
  listFails = true;
  from = fetches.length;
  await page.renderAlertRuleList(main, { poll: true });
  await settle();
  listFails = false;
  out.failedPoll = { names: namesOf(main), menuKept: templateMenu(main) === menuBefore && templateMenu(main).open === true, refreshError: withClass(main, "refresh-error").map((n) => n.textContent), requests: since(from) };

  /* The next poll that succeeds takes the failure notice down again. */
  await page.renderAlertRuleList(main, { poll: true });
  await settle();
  out.recoveredPoll = { refreshError: withClass(main, "refresh-error").length, names: namesOf(main) };
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
process.stdout.write(JSON.stringify(out));
