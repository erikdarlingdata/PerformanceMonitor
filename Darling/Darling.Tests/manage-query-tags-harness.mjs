/* Runs the shipped Manage Query Tags page (wwwroot/js/pages/manage-query-tags.js) under Node against a fake DOM and a fake
   fetch, and prints the result of a scenario as one line of JSON. ManageQueryTagsBehaviourTests starts it as
       node manage-query-tags-harness.mjs <path to wwwroot/js> <scenario>
   The page and the modules it imports are copied unchanged into a scratch folder and imported as the browser would. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [jsDir, scenarioArg] = process.argv.slice(-2);
const [scenario, scenarioParam] = scenarioArg.split(":");

class FakeNode {
  constructor(tag, text) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.style = {};
    this.className = "";
    this.value = "";
    this.handlers = {};
    this.isConnected = true;
    this.text = text == null ? null : String(text);
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
  get firstChild() { return this.children[0] || null; }
  appendChild(child) { this.children.push(child); return child; }
  removeChild(child) {
    const i = this.children.indexOf(child);
    if (i >= 0) this.children.splice(i, 1);
    return child;
  }
  setAttribute(name, value) {
    this.attrs[name] = String(value);
    if (name === "value") this.value = String(value);
  }
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
};
let confirmAnswer = true;
globalThis.window = globalThis;
globalThis.confirm = () => confirmAnswer;
globalThis.location = { hash: "", pathname: "/", search: "", href: "http://localhost/", origin: "http://localhost" };
globalThis.history = { pushState() {}, replaceState() {}, back() {} };

const all = (node, pred, out = []) => {
  if (pred(node)) out.push(node);
  for (const c of node.children) all(c, pred, out);
  return out;
};
const buttons = (root, text) => all(root, (n) => n.tag === "button" && n.textContent === text);
const fire = async (node, type, e = {}) => {
  for (const fn of node.handlers[type] || []) await fn(e);
  await settle();
};
const settle = () => new Promise((r) => setTimeout(r, 15));
const clickText = async (root, text) => {
  const b = buttons(root, text)[0];
  if (!b) throw new Error("no button " + text);
  await fire(b, "click");
};
const field = (root, name) => all(root, (n) => n.attrs["data-field"] === name)[0];
const typeInto = async (root, name, v) => { const f = field(root, name); f.value = v; await fire(f, "input"); };
const nodeButton = (root, id) => all(root, (n) => n.attrs["data-tag-id"] === String(id))[0];
const box = (root, id) => all(root, (n) => n.attrs["data-server-id"] === String(id))[0];
const tick = async (root, node, checked) => { node.checked = checked; await fire(node, "change"); };

const qtag = (tag_id, name, parent_id, assignments = [], rule_count = 0) => ({ tag_id, name, parent_id, colour: null, path: name, assignment_count: assignments.length, rule_count, assignments });
const hashA = { assignment_id: 1, tag_id: 1, kind: "hash", server_id: null, server_name: null, query_hash: "0xAB12", database_name: null, schema_name: null, object_name: null };
const objA = { assignment_id: 2, tag_id: 1, kind: "object", server_id: 7, server_name: "srv-g", query_hash: null, database_name: "db<i>", schema_name: "dbo", object_name: "proc1" };
let tagList = [qtag(1, "Reports", null, [hashA, objA], 1), qtag(2, "Batch<b>", 1)];
let rulesList = [
  { id: 5, field: "login", pattern: "svc*", priority: 100, enabled: true, tag_id: 1, tag: "Reports", server_id: null },
  { id: 6, field: "application", pattern: "etl*", priority: 10, enabled: false, tag_id: 2, tag: "Reports/Batch<b>", server_id: 7 },
];
/* The fleet's cards (/api/fleet): the page names a server by the card with its server_id. */
let fleetCards = [
  { server_id: 7, server_name: "srv-g", display_name: "Server G" },
  { server_id: 8, server_name: "srv-h", display_name: "Server H" },
  { server_id: 9, server_name: "srv-i", display_name: "" },
];
let fleetDown = false;
const ruleReads = [];
let canEdit = true;
let gate = null;
let responder = () => ({ status: 200, body: {} });
const calls = [];
const map = { current: true, rules_version: 3, built_version: 3 };
globalThis.fetch = async (url, init = {}) => {
  const method = init.method || "GET";
  const body = init.body ? JSON.parse(init.body) : null;
  if (url === "/api/session") return reply(200, { can_edit: canEdit });
  if (url === "/api/read/list_query_tags") return reply(200, { status: "ok", tag_count: tagList.length, tags: tagList, map });
  if (url === "/api/fleet") return fleetDown ? reply(500, { error: "fleet down" }) : reply(200, { cards: fleetCards, tags: [] });
  if ((url === "/api/query-tag-rules" || url.startsWith("/api/query-tag-rules?")) && method === "GET") {
    ruleReads.push(url);
    const asked = url.includes("?server_id=") ? Number(url.split("?server_id=")[1]) : null;
    const rules = asked == null ? rulesList : rulesList.filter((r) => r.server_id == null || r.server_id === asked);
    return reply(200, { status: "ok", count: rules.length, rules_version: 3, built_version: 3, rules });
  }
  calls.push({ method, url, body, contentType: (init.headers || {})["Content-Type"] || null });
  if (gate) await gate;
  const r = responder(method, url, body);
  return reply(r.status, r.body);
};
const reply = (status, body) => ({ status, ok: status >= 200 && status < 300, text: async () => JSON.stringify(body) });

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "manage-query-tags-"));
const out = {};
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const page = await import(pathToFileURL(path.join(scratch, "pages", "manage-query-tags.js")).href);
  const main = new FakeNode("main");
  const mountPage = async () => { await page.renderManageQueryTags(main); await settle(); };
  const paint = () => main.textContent;
  const pickServer = async (name, v) => { const f = field(main, name); f.value = v; await fire(f, "change"); };
  const items = (attr) => all(main, (n) => n.tag === "li" && n.attrs[attr] !== undefined);

  const scenarios = {
    render: async () => {
      await mountPage();
      await fire(nodeButton(main, 1), "click");
      return {
        tree: all(main, (n) => n.attrs["data-tag-id"] !== undefined).map((n) => n.textContent),
        assignments: items("data-assignment-id").map((n) => n.textContent),
        rules: items("data-rule-id").map((n) => n.attrs["data-rule-id"]),
        text: paint(),
      };
    },
    createTag: async () => {
      responder = () => ({ status: 201, body: { status: "created", tag: { tag_id: 9, name: "New" } } });
      await mountPage();
      await clickText(main, "New root tag");
      await typeInto(main, "name", "  New ");
      await clickText(main, "Save");
      return { calls, text: paint() };
    },
    invalid: async () => {
      responder = () => ({ status: 400, body: { status: "invalid", refusal: "invalid_name", message: "A tag name must not be blank." } });
      await mountPage();
      await clickText(main, "New root tag");
      await typeInto(main, "name", "X");
      await clickText(main, "Save");
      return { text: paint(), formOpen: buttons(main, "Save").length };
    },
    delete: async () => {
      responder = (m, u) => u.includes("confirm=true")
        ? { status: 200, body: { status: "deleted", tag_id: 1, deleted_tag_ids: [1, 2] } }
        : { status: 409, body: { status: "confirm_required", refusal: "tag_in_use", message: "Tag 1 has 1 child tag(s).", child_tags: 1, assignments: 2, rules: 1 } };
      await mountPage();
      await fire(nodeButton(main, 1), "click");
      await clickText(main, "Delete");
      const first = { calls: calls.length, text: paint(), anyway: buttons(main, "Delete anyway").length };
      await clickText(main, "Delete anyway");
      return { first, calls };
    },
    assignHash: async () => {
      responder = () => ({ status: 200, body: { status: "assigned" } });
      await mountPage();
      await fire(nodeButton(main, 2), "click");
      await clickText(main, "Assign a query");
      await typeInto(main, "query_hash", " 0xCD34 ");
      await pickServer("server_id", "7");
      await clickText(main, "Save");
      return { calls };
    },
    assignObject: async () => {
      responder = () => ({ status: 200, body: { status: "moved" } });
      await mountPage();
      await fire(nodeButton(main, 2), "click");
      await clickText(main, "Assign a query");
      const kind = field(main, "kind");
      kind.value = "object";
      await fire(kind, "change");
      await typeInto(main, "database_name", "db1");
      await typeInto(main, "schema_name", "dbo");
      await typeInto(main, "object_name", "p");
      await clickText(main, "Save");
      return { calls, text: paint() };
    },
    unassign: async () => {
      responder = () => ({ status: 200, body: { status: "unassigned" } });
      await mountPage();
      await fire(nodeButton(main, 1), "click");
      await fire(all(main, (n) => n.attrs["aria-label"] === "Remove assignment db<i>.dbo.proc1")[0], "click");
      return { calls };
    },
    ruleCreate: async () => {
      responder = () => ({ status: 201, body: { status: "created", rule: { id: 8 } } });
      await mountPage();
      await clickText(main, "New rule");
      await typeInto(main, "pattern", "rep*");
      const tagSel = field(main, "tag_id");
      tagSel.value = "1";
      await fire(tagSel, "change");
      await typeInto(main, "priority", "50");
      await clickText(main, "Save");
      return { calls, text: paint().slice(0, 400) };
    },
    ruleTest: async () => {
      responder = (m, u) => ({ status: 200, body: { status: "ok", message: "Nothing was written.", keys_examined: 12, keys_matched: 5, would_tag: 3, held_by_hand_assignment: 1, held_by_higher_rule: 1, samples: [{ outcome: "would_tag", key: "0xAA", matched_field: "application", matched_value: "<script>x</script>" }] } });
      const real = globalThis.fetch;
      globalThis.fetch = async (url, init = {}) => {
        if (String(url).startsWith("/api/query-tag-rules/test")) { calls.push({ method: init.method || "GET", url }); return reply(200, responder().body); }
        return real(url, init);
      };
      await mountPage();
      await clickText(main, "New rule");
      await typeInto(main, "pattern", "rep*");
      await clickText(main, "Test");
      return { calls, text: paint() };
    },
    ruleEdit: async () => {
      responder = () => ({ status: 200, body: { status: "updated" } });
      await mountPage();
      await fire(all(main, (n) => n.attrs["aria-label"] === "Edit rule 6")[0], "click");
      const enabled = field(main, "enabled");
      enabled.checked = true;
      await fire(enabled, "change");
      await clickText(main, "Save");
      return { calls };
    },
    ruleServerNames: async () => {
      rulesList = [...rulesList, { id: 7, field: "host", pattern: "gone*", priority: 20, enabled: true, tag_id: 1, tag: "Reports", server_id: 99 }, { id: 8, field: "host", pattern: "nodisp*", priority: 30, enabled: true, tag_id: 1, tag: "Reports", server_id: 9 }];
      await mountPage();
      return { rules: items("data-rule-id").map((n) => n.textContent), options: all(field(main, "rule_server_filter"), (n) => n.tag === "option").map((n) => n.textContent) };
    },
    ruleServerFilter: async () => {
      await mountPage();
      const first = ruleReads.slice();
      await pickServer("rule_server_filter", "8");
      const picked = { reads: ruleReads.slice(), rules: items("data-rule-id").map((n) => n.attrs["data-rule-id"]), shown: field(main, "rule_server_filter").value };
      await page.renderManageQueryTags(main);
      await settle();
      const tickRead = ruleReads[ruleReads.length - 1];
      const afterTick = { shown: field(main, "rule_server_filter").value, rules: items("data-rule-id").map((n) => n.attrs["data-rule-id"]) };
      await pickServer("rule_server_filter", "");
      return { first, picked, tickRead, afterTick, back: { read: ruleReads[ruleReads.length - 1], rules: items("data-rule-id").map((n) => n.attrs["data-rule-id"]) } };
    },
    fleetDown: async () => {
      fleetDown = true;
      await mountPage();
      await fire(nodeButton(main, 1), "click");
      await clickText(main, "New rule");
      const f = field(main, "server_id");
      return { rules: items("data-rule-id").map((n) => n.textContent), serverInput: f.tag, assignments: items("data-assignment-id").map((n) => n.textContent) };
    },
    ruleServerPick: async () => {
      responder = () => ({ status: 201, body: { status: "created", rule: { id: 8 } } });
      await mountPage();
      await clickText(main, "New rule");
      const f = field(main, "server_id");
      const options = all(f, (n) => n.tag === "option").map((n) => n.textContent);
      await typeInto(main, "pattern", "rep*");
      await pickServer("server_id", "8");
      await clickText(main, "Save");
      return { tag: f.tag, options, calls };
    },
    pollKeepsForm: async () => {
      await mountPage();
      await clickText(main, "New root tag");
      await typeInto(main, "name", "Half typed");
      const input = field(main, "name");
      tagList = [qtag(1, "Reports", null, [hashA], 1)];
      await page.renderManageQueryTags(main);
      await settle();
      return { sameInput: field(main, "name") === input, value: field(main, "name").value, saves: buttons(main, "Save").length };
    },
    doubleClick: async () => {
      let release;
      gate = new Promise((r) => { release = r; });
      responder = () => ({ status: 201, body: { status: "created", tag: { tag_id: 9 } } });
      await mountPage();
      await clickText(main, "New root tag");
      await typeInto(main, "name", "Again");
      const save = buttons(main, "Save")[0];
      const first = fire(save, "click");
      await fire(save, "click");
      release();
      await first;
      return { calls: calls.length };
    },
    readonly: async () => {
      canEdit = false;
      await mountPage();
      await fire(nodeButton(main, 1), "click");
      return {
        writes: ["New root tag", "New rule", "New child tag", "Edit", "Delete", "Assign a query", "Edit rule", "Delete rule"].map((t) => all(main, (n) => n.tag === "button" && n.textContent.startsWith(t)).length),
        tree: all(main, (n) => n.attrs["data-tag-id"] !== undefined).length,
        rules: items("data-rule-id").length,
      };
    },
  };
  Object.assign(out, await scenarios[scenario]());
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
console.log(JSON.stringify(out));
