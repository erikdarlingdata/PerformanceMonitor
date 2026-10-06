/* Runs the shipped Manage Tags page (wwwroot/js/pages/manage-tags.js) under Node against a fake DOM and a fake
   fetch, and prints the result of a scenario as one line of JSON. ManageTagsBehaviourTests starts it as
       node manage-tags-harness.mjs <path to wwwroot/js> <scenario>
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

const tag = (id, name, parent_id, sort_order, colour) => ({ id, name, parent_id, sort_order, colour });
const forest = [tag(1, "East", null, 0, "#4E79A7"), tag(2, "Prod", 1, 0, null), tag(3, "West", null, 1, "#E15759"), tag(4, "Edge<b>", 2, 0, "javascript:x")];
const card = (server_id, display_name, tags) => ({ server_id, display_name, tags });
let cards = [card(10, "srv-a", [{ id: 1 }]), card(11, "srv-b", []), card(12, "srv-c", [{ id: 1 }])];

let canEdit = true;
let networkDown = false;
let gate = null;
let responder = () => ({ status: 200, body: {} });
const calls = [];
globalThis.fetch = async (url, init = {}) => {
  const method = init.method || "GET";
  const body = init.body ? JSON.parse(init.body) : null;
  if (url === "/api/session") return reply(200, { can_edit: canEdit });
  if (url === "/api/fleet") return reply(200, { tags: forest, cards });
  calls.push({ method, url, body, contentType: (init.headers || {})["Content-Type"] || null });
  if (networkDown) throw new Error("connection refused");
  if (gate) await gate;
  const r = responder(method, url, body);
  return reply(r.status, r.body);
};
const reply = (status, body) => ({ status, ok: status >= 200 && status < 300, text: async () => JSON.stringify(body) });

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "manage-tags-"));
const out = {};
try {
  /* Copy the whole js tree (js/, js/pages/ and every subdirectory) rather than a hand-kept list (#5279): a page module that
     another PR adds then needs no edit here. Only imported files load, so the rest are inert. */
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const page = await import(pathToFileURL(path.join(scratch, "pages", "manage-tags.js")).href);
  const main = new FakeNode("main");
  const mountPage = async () => { await page.renderManageTags(main); await settle(); };
  const paint = () => main.textContent;

  const scenarios = {
    render: async () => {
      await mountPage();
      const nodes = all(main, (n) => n.attrs["data-tag-id"] !== undefined);
      return {
        order: nodes.map((n) => n.textContent),
        padding: all(main, (n) => n.tag === "li" && n.attrs.style).map((n) => n.attrs.style),
        swatches: all(main, (n) => n.tag === "span" && n.className === "tag-swatch").map((n) => n.attrs.style || null),
        newRoot: buttons(main, "New root tag").length,
      };
    },
    create: async () => {
      responder = (m) => m === "POST" ? { status: 201, body: { status: "created", tag: { tag_id: 9, name: "New" }, affected_rules: [{ rule_id: "r1", name: "Disk rule", effect: "gained", servers_gained_count: 2 }] } } : { status: 200, body: {} };
      await mountPage();
      await clickText(main, "New root tag");
      await typeInto(main, "name", "  New ");
      await typeInto(main, "colour", "#112233");
      await clickText(main, "Save");
      return { calls, text: paint() };
    },
    child: async () => {
      responder = () => ({ status: 201, body: { status: "created", tag: { tag_id: 9 }, affected_rules: [] } });
      await mountPage();
      await fire(nodeButton(main, 3), "click");
      await clickText(main, "New child tag");
      await typeInto(main, "name", "Kid");
      await clickText(main, "Save");
      return { calls };
    },
    rename: async () => {
      responder = () => ({ status: 409, body: { status: "conflict", refusal: "duplicate_name", message: "A tag named 'West' already exists under this parent." } });
      await mountPage();
      await fire(nodeButton(main, 1), "click");
      await clickText(main, "Edit");
      await typeInto(main, "name", "West");
      await clickText(main, "Save");
      return { calls, text: paint(), formOpen: buttons(main, "Save").length };
    },
    patch: async () => {
      responder = () => ({ status: 200, body: { status: "updated", tag: { tag_id: 2 }, affected_rules: [] } });
      await mountPage();
      await fire(nodeButton(main, 2), "click");
      await clickText(main, "Edit");
      await typeInto(main, "colour", "#aabbcc");
      const parent = field(main, "parent_id");
      parent.value = "";
      await fire(parent, "change");
      const parentOptions = all(parent, (n) => n.tag === "option").map((o) => o.attrs.value);
      await clickText(main, "Save");
      return { calls, parentOptions };
    },
    invalid: async () => {
      responder = () => ({ status: 400, body: { status: "invalid", message: "'colour' must be #RRGGBB." } });
      await mountPage();
      await clickText(main, "New root tag");
      await typeInto(main, "name", "X");
      await clickText(main, "Save");
      return { text: paint() };
    },
    delete: async () => {
      let n = 0;
      responder = (m, u) => {
        n++;
        return u.includes("confirm=true")
          ? { status: 200, body: { status: "deleted", tag_id: 3, deleted_tag_ids: [3], affected_rules: [{ rule_id: "r2", name: "West rule", effect: "lost", servers_lost_count: 1 }] } }
          : { status: 409, body: { status: "confirm_required", refusal: "rules_affected", message: "Deleting tag 3 would leave 1 rule matching no server.", affected_rules: [{ rule_id: "r2", name: "West rule" }, { rule_id: "r3", name: "Other rule" }] } };
      };
      await mountPage();
      await fire(nodeButton(main, 3), "click");
      await clickText(main, "Delete");
      const afterFirst = { calls: calls.length, text: paint(), anyway: buttons(main, "Delete anyway").length };
      await clickText(main, "Delete anyway");
      return { afterFirst, calls, text: paint(), selectedAfter: all(main, (x) => x.attrs["aria-current"] === "true").length };
    },
    deleteDeclined: async () => {
      confirmAnswer = false;
      await mountPage();
      await fire(nodeButton(main, 3), "click");
      await clickText(main, "Delete");
      return { calls: calls.length };
    },
    assign: async () => {
      responder = () => ({ status: 200, body: { status: "assigned", affected_rules: [{ rule_id: "r9", name: "Cover rule", effect: "gained", servers_gained_count: 1 }] } });
      await mountPage();
      await fire(nodeButton(main, 1), "click");
      const before = [10, 11, 12].map((id) => box(main, id).checked);
      await tick(main, box(main, 11), true);
      await tick(main, box(main, 12), false);
      await clickText(main, "Apply");
      return { before, calls, text: paint() };
    },
    repaint: async () => {
      await mountPage();
      await fire(nodeButton(main, 1), "click");
      await tick(main, box(main, 11), true);
      await mountPage();
      return {
        pressed: all(main, (n) => n.attrs["aria-current"] === "true").map((n) => n.attrs["data-tag-id"]),
        checked: [10, 11, 12].map((id) => box(main, id).checked),
        calls: calls.length,
      };
    },
    staleEdit: async () => {
      responder = () => ({ status: 200, body: { status: "assigned", affected_rules: [] } });
      await mountPage();
      await fire(nodeButton(main, 1), "click");
      await tick(main, box(main, 11), true);
      cards = [card(10, "srv-a", [{ id: 1 }]), card(11, "srv-b", []), card(12, "srv-c", [{ id: 1 }]), card(13, "srv-d", [{ id: 1 }])];
      await mountPage();
      const checkedAfterTick = [10, 11, 12, 13].map((id) => box(main, id).checked);
      await clickText(main, "Apply");
      return { checkedAfterTick, calls };
    },
    tickForm: async () => {
      await mountPage();
      await clickText(main, "New root tag");
      await typeInto(main, "name", "Typed");
      const before = field(main, "name");
      await mountPage();
      const after = field(main, "name");
      return { same: before === after, value: after.value, forms: all(main, (n) => n.attrs["data-field"] === "name").length };
    },
    tickParents: async () => {
      await mountPage();
      await clickText(main, "New root tag");
      await typeInto(main, "name", "Typed");
      const before = field(main, "name");
      forest.push(tag(5, "Added", null, 2, null));
      await mountPage();
      const options = all(field(main, "parent_id"), (n) => n.tag === "option").map((o) => o.attrs.value);
      forest.pop();
      return { same: before === field(main, "name"), options };
    },
    dblDelete: async () => {
      responder = (m, u) => u.includes("confirm=true")
        ? { status: 200, body: { status: "deleted", deleted_tag_ids: [3], affected_rules: [{ rule_id: "r2", name: "West rule" }] } }
        : { status: 409, body: { status: "confirm_required", message: "Would orphan.", affected_rules: [{ rule_id: "r2", name: "West rule" }] } };
      await mountPage();
      await fire(nodeButton(main, 3), "click");
      await clickText(main, "Delete");
      const go = buttons(main, "Delete anyway")[0];
      let release;
      gate = new Promise((r) => { release = r; });
      const errors = [];
      const run = () => Promise.resolve().then(() => go.handlers.click[0]()).catch((e) => errors.push(String(e)));
      const p1 = run();
      const p2 = run();
      await settle();
      gate = null;
      release();
      await Promise.all([p1, p2]);
      await settle();
      return { calls, errors, text: paint() };
    },
    dblSave: async () => {
      responder = () => ({ status: 201, body: { status: "created", tag: { tag_id: 9 }, affected_rules: [] } });
      await mountPage();
      await clickText(main, "New root tag");
      await typeInto(main, "name", "Dup");
      const save = buttons(main, "Save")[0];
      let release;
      gate = new Promise((r) => { release = r; });
      const errors = [];
      const run = () => Promise.resolve().then(() => save.handlers.click[0]()).catch((e) => errors.push(String(e)));
      const p1 = run();
      const p2 = run();
      await settle();
      gate = null;
      release();
      await Promise.all([p1, p2]);
      await settle();
      return { calls, errors };
    },
    dblApply: async () => {
      responder = () => ({ status: 200, body: { status: "assigned", affected_rules: [] } });
      await mountPage();
      await fire(nodeButton(main, 1), "click");
      await tick(main, box(main, 11), true);
      await tick(main, box(main, 12), false);
      const apply = buttons(main, "Apply")[0];
      let release;
      gate = new Promise((r) => { release = r; });
      const errors = [];
      const run = () => Promise.resolve().then(() => apply.handlers.click[0]()).catch((e) => errors.push(String(e)));
      const p1 = run();
      const p2 = run();
      await settle();
      gate = null;
      release();
      await Promise.all([p1, p2]);
      await settle();
      return { calls, errors };
    },
    editGone: async () => {
      responder = () => ({ status: 404, body: { status: "not_found", message: "No such tag." } });
      await mountPage();
      await fire(nodeButton(main, 3), "click");
      await clickText(main, "Edit");
      await typeInto(main, "name", "Renamed");
      forest.splice(2, 1);
      await clickText(main, "Save");
      const out = { text: paint(), formOpen: buttons(main, "Save").length, node: all(main, (n) => n.attrs["data-tag-id"] === "3").length };
      forest.splice(2, 0, tag(3, "West", null, 1, "#E15759"));
      return out;
    },
    partial: async () => {
      responder = (m) => m === "POST"
        ? { status: 200, body: { status: "assigned", affected_rules: [{ rule_id: "r7", name: "Gained rule", effect: "gained" }] } }
        : { status: 500, body: { message: "Store unavailable." } };
      await mountPage();
      await fire(nodeButton(main, 1), "click");
      await tick(main, box(main, 11), true);
      await tick(main, box(main, 12), false);
      const fleetBefore = calls.length;
      await clickText(main, "Apply");
      return { calls: calls.length - fleetBefore, text: paint() };
    },
    refusal: async () => {
      const status = Number(scenarioParam);
      responder = () => status === 0 ? { status: 200, body: {} } : { status, body: status === 403 ? { error: "Read-only sign-in." } : status === 404 ? { status: "not_found", refusal: "unknown_parent", message: "No such tag." } : status === 415 ? { error: "Content-Type must be application/json." } : { message: "Boom." } };
      networkDown = status === 0;
      await mountPage();
      await clickText(main, "New root tag");
      await typeInto(main, "name", "Keep me");
      await clickText(main, "Save");
      const name = field(main, "name");
      return { text: paint(), formOpen: buttons(main, "Save").length, kept: name ? name.value : null };
    },
    editParent: async () => {
      responder = () => ({ status: 404, body: { status: "not_found", refusal: "unknown_parent", message: "Parent tag 2 does not exist." } });
      await mountPage();
      await fire(nodeButton(main, 3), "click");
      await tick(main, box(main, 11), true);
      await clickText(main, "Edit");
      await typeInto(main, "name", "Renamed");
      const sel = field(main, "parent_id");
      sel.value = "1";
      await fire(sel, "change");
      await clickText(main, "Save");
      const name = field(main, "name");
      return { text: paint(), formOpen: buttons(main, "Save").length, kept: name ? name.value : null, parent: field(main, "parent_id").value, apply: buttons(main, "Apply").length, sel: all(main, (x) => x.attrs["aria-current"] === "true").length };
    },
    busyCancel: async () => {
      responder = () => ({ status: 201, body: { status: "created", tag: { tag_id: 9 }, affected_rules: [] } });
      await mountPage();
      await clickText(main, "New root tag");
      await typeInto(main, "name", "Slow");
      const openers = buttons(main, "New root tag");
      let release;
      gate = new Promise((r) => { release = r; });
      const p = Promise.resolve().then(() => buttons(main, "Save")[0].handlers.click[0]());
      await settle();
      const during = { cancel: buttons(main, "Cancel")[0].disabled, opener: openers[0].disabled };
      gate = null;
      release();
      await p;
      await settle();
      return { during, after: openers[0].disabled };
    },
    secondSave: async () => {
      responder = () => ({ status: 500, body: { message: "Boom." } });
      await mountPage();
      await clickText(main, "New root tag");
      await typeInto(main, "name", "Again");
      await clickText(main, "Save");
      await clickText(main, "Save");
      return { calls: calls.length };
    },
    baselineAgrees: async () => {
      responder = () => ({ status: 200, body: { status: "assigned", affected_rules: [] } });
      await mountPage();
      await fire(nodeButton(main, 1), "click");
      await tick(main, box(main, 11), true);
      const apply = buttons(main, "Apply")[0];
      cards = [card(10, "srv-a", [{ id: 1 }]), card(11, "srv-b", [{ id: 1 }]), card(12, "srv-c", [{ id: 1 }])];
      await page.renderManageTags(main);
      await settle();
      const before = calls.length;
      await fire(apply, "click");
      return { requests: calls.length - before, text: paint() };
    },
    deleteRefusal: async () => {
      const status = Number(scenarioParam);
      responder = () => ({ status, body: { message: "Refused " + status + "." } });
      await mountPage();
      await fire(nodeButton(main, 3), "click");
      await clickText(main, "Delete");
      return { text: paint(), selected: all(main, (x) => x.attrs["aria-current"] === "true").length, calls: calls.length };
    },
    readonly: async () => {
      canEdit = false;
      await mountPage();
      await fire(nodeButton(main, 1), "click");
      return {
        newRoot: buttons(main, "New root tag").length,
        writes: ["New child tag", "Edit", "Delete", "Apply", "Reset", "Save"].map((t) => buttons(main, t).length),
        disabled: [10, 11, 12].map((id) => box(main, id).disabled),
        tree: all(main, (n) => n.attrs["data-tag-id"] !== undefined).length,
      };
    },
  };
  Object.assign(out, await scenarios[scenario]());
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
console.log(JSON.stringify(out));
