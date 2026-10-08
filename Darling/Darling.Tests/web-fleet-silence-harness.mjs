/* Runs the shipped Fleet page (wwwroot/js/pages/fleet.js) under Node against a fake DOM and a fake fetch and
   prints a scenario's result as one line of JSON. FleetSilenceBehaviourTests starts it as
       node web-fleet-silence-harness.mjs <path to wwwroot/js> <scenario>
   The page and every module are copied unchanged into a scratch folder and imported as the browser would. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [jsDir, scenario] = process.argv.slice(-2);

class FakeNode {
  constructor(tag, text) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.style = {};
    this.className = "";
    this.handlers = {};
    this.isConnected = true;
    this.text = text == null ? null : String(text);
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
  get firstChild() { return this.children[0] || null; }
  appendChild(child) { this.children.push(child); return child; }
  removeChild(child) { const i = this.children.indexOf(child); if (i >= 0) this.children.splice(i, 1); return child; }
  setAttribute(name, value) { this.attrs[name] = String(value); }
  getAttribute(name) { return name in this.attrs ? this.attrs[name] : null; }
  addEventListener(type, fn) { (this.handlers[type] ||= []).push(fn); }
  set textContent(value) { this.children = []; this.text = String(value); }
  get textContent() { return (this.text || "") + this.children.map((c) => c.textContent).join(""); }
}

globalThis.Node = FakeNode;
globalThis.document = {
  createElement: (tag) => new FakeNode(tag),
  createElementNS: (ns, tag) => new FakeNode(tag),
  createTextNode: (text) => new FakeNode("#text", text),
  getElementById: () => null, querySelector: () => null, querySelectorAll: () => [], addEventListener() {},
};
const confirms = [];
let confirmAnswer = true;
globalThis.window = globalThis;
globalThis.confirm = (m) => { confirms.push(m); return confirmAnswer; };
globalThis.location = { hash: "", pathname: "/", search: "", href: "http://localhost/", origin: "http://localhost" };
globalThis.history = { pushState() {}, replaceState() {}, back() {} };
const mem = new Map();
globalThis.localStorage = { getItem: (k) => (mem.has(k) ? mem.get(k) : null), setItem: (k, v) => mem.set(k, String(v)), removeItem: (k) => mem.delete(k) };
globalThis.sessionStorage = globalThis.localStorage;
globalThis.addEventListener = () => {};
globalThis.matchMedia = () => ({ matches: false, addEventListener() {}, removeEventListener() {} });

const all = (node, pred, out = []) => { if (pred(node)) out.push(node); for (const c of node.children) all(c, pred, out); return out; };
const settle = () => new Promise((r) => setTimeout(r, 25));
const silenceButtons = (root) => all(root, (n) => n.tag === "button" && /^(Silence|Unsilence|…)$/.test(n.textContent));
const bells = (root) => all(root, (n) => n.className === "silenced-bell").length;
const click = async (node, stop = () => {}) => {
  for (const fn of node.handlers.click || []) await fn({ stopPropagation: stop });
  await settle();
};

const card = (server_id, display_name, is_silenced) => ({
  server_id, display_name, server_name: display_name, band: "Healthy", band_label: "Healthy", is_online: true,
  status: "ok", last_collection: null, tags: [], is_silenced, metric_count: 0, measured_metric_count: 0,
});
let canEdit = true;
let fleetSilenced = false;
let rules = [];
let gate = null;
let extraCards = null;
const calls = [];
const reply = (status, body) => ({ status, ok: status >= 200 && status < 300, text: async () => JSON.stringify(body) });
globalThis.fetch = async (url, init = {}) => {
  const method = init.method || "GET";
  if (url === "/api/session") return reply(200, { can_edit: canEdit });
  if (url === "/api/fleet") return reply(200, { total_servers: extraCards ? extraCards.length : 1, critical_count: 0, warning_count: 0, offline_count: 0, tags: [], cards: extraCards || [card(10, "srv-a", fleetSilenced)] });
  if (url.startsWith("/api/read/get_mute_rules")) { calls.push({ method, url }); return reply(200, { mute_rules: rules }); }
  calls.push({ method, url, body: init.body ? JSON.parse(init.body) : null });
  if (gate) await gate;
  return reply(method === "POST" ? 201 : 200, { status: method === "POST" ? "created" : "deleted", mute_rule: { id: "new" } });
};

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "fleet-silence-"));
const out = {};
try {
  /* Copy the whole js tree (js/, js/pages/ and every subdirectory) rather than a hand-kept list (#5279): a page module that
     another PR adds then needs no edit here. Only imported files load, so the rest are inert. */
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const page = await import(pathToFileURL(path.join(scratch, "pages", "fleet.js")).href);
  const main = new FakeNode("main");
  const draw = async () => { await page.renderFleet(main); await settle(); };
  const writes = () => calls.filter((c) => c.method !== "GET");

  const scenarios = {
    gate: async () => {
      canEdit = false;
      await draw();
      return { buttons: silenceButtons(main).length };
    },
    silence: async () => {
      await draw();
      const before = silenceButtons(main).map((b) => b.textContent);
      await click(silenceButtons(main)[0]);
      const afterClick = silenceButtons(main).map((b) => b.textContent);
      /* the 60 s poll: the fleet read still says not silenced (it predates the write) */
      await draw();
      return { before, afterClick, afterRebuild: silenceButtons(main).map((b) => b.textContent), bell: bells(main), writes: writes(), confirms };
    },
    declined: async () => {
      confirmAnswer = false;
      await draw();
      await click(silenceButtons(main)[0]);
      return { writes: writes().length, confirms: confirms.length };
    },
    pending: async () => {
      let release;
      gate = new Promise((r) => { release = r; });
      await draw();
      const first = click(silenceButtons(main)[0]);
      await settle();
      await draw(); /* a rebuild while the write is in flight */
      const rebuilt = silenceButtons(main)[0];
      const disabled = rebuilt.attrs.disabled === "disabled";
      await click(rebuilt); /* a second click on the rebuilt card must not fire again */
      release();
      await first;
      await settle();
      return { disabled, posts: writes().filter((c) => c.method === "POST").length, label: silenceButtons(main)[0].textContent };
    },
    unsilence: async () => {
      fleetSilenced = true;
      const own = { id: "own-1", server_id: 10, reason: "Silenced from server list", metric_name: null, database_pattern: null, query_text_pattern: null, wait_type_pattern: null, job_name_pattern: null };
      rules = [
        own,
        { ...own, id: "narrow", metric_name: "Blocking" },
        { ...own, id: "other-server", server_id: 11 },
        { ...own, id: "hand-built", reason: "my own words" },
        { ...own, id: "other-name", server_id: null, server_name: "elsewhere" },
      ];
      await draw();
      const before = silenceButtons(main).map((b) => b.textContent);
      await click(silenceButtons(main)[0]);
      await draw(); /* the fleet read still says silenced for a moment */
      return { before, writes: writes(), after: silenceButtons(main).map((b) => b.textContent), confirms };
    },
    /* the invariant: every server the page shows as silenced (the bell's test, no reason condition) loses its rule */
    unsilenceAgrees: async () => {
      const none = { metric_name: null, database_pattern: null, query_text_pattern: null, wait_type_pattern: null, job_name_pattern: null };
      rules = [
        { id: "r-marker", server_id: 20, server_name: "srv-20", reason: "Silenced from server list", ...none },
        { id: "r-edited", server_id: 21, server_name: "srv-21", reason: "my own words", ...none },
        { id: "r-noreason", server_id: 22, server_name: "srv-22", reason: null, ...none },
        { id: "r-narrow", server_id: 23, server_name: "srv-23", reason: "x", ...none, metric_name: "Blocking" },
        { id: "r-name", server_id: null, server_name: "SRV-24", reason: "legacy", ...none },
      ];
      extraCards = [20, 21, 22, 24].map((i) => card(i, "srv-" + i, true));
      await draw();
      const perServer = {};
      for (let i = 0; i < 4; i++) {
        const before = writes().length;
        await click(silenceButtons(main)[i]);
        perServer[extraCards[i].server_id] = writes().slice(before).map((c) => c.method + " " + c.url);
      }
      return { perServer, notices: all(main, (n) => n.attrs.role === "status").map((n) => n.textContent) };
    },
    unsilenceNameOnly: async () => {
      fleetSilenced = true;
      rules = [{ id: "legacy", server_id: null, server_name: "SRV-A", reason: "x", metric_name: null, database_pattern: null, query_text_pattern: null, wait_type_pattern: null, job_name_pattern: null }];
      await draw();
      await click(silenceButtons(main)[0]);
      return { writes: writes().length, notice: all(main, (n) => n.attrs.role === "status").map((n) => n.textContent) };
    },
    noticeClears: async () => {
      await draw();
      await click(silenceButtons(main)[0]);
      const shown = all(main, (n) => n.attrs.role === "status").length;
      await draw();
      return { shown, afterRebuild: all(main, (n) => n.attrs.role === "status").length };
    },
    hung: async () => {
      page.silenceTimeouts.writeMs = 50;
      gate = new Promise(() => {}); /* the write never returns */
      await draw();
      await click(silenceButtons(main)[0]);
      await new Promise((r) => setTimeout(r, 150));
      const b = silenceButtons(main)[0];
      return { disabled: b.attrs.disabled === "disabled", label: b.textContent, notice: all(main, (n) => n.attrs.role === "status").map((n) => n.textContent) };
    },
  };
  if (!scenarios[scenario]) throw new Error("unknown scenario " + scenario);
  Object.assign(out, await scenarios[scenario]());
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
console.log(JSON.stringify(out));
process.exit(0);
