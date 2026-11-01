/* Runs the shipped Admin "People" tab (wwwroot/js/pages/admin-people.js) with the real util.js on a small fake DOM and a scripted fetch,
   and prints as one line of JSON what a person would see and what the page sent (#7107).
   WebPeopleBehaviourTests starts it as
       node web-people-harness.mjs <path to wwwroot/js> <scenario>
   Stand-ins: the DOM, fetch, the clipboard, panels.js (its table draws each row's name and Details cell) and pages/admin.js. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag) { this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = ""; this.className = ""; this.dataset = {}; this.disabled = false; this.value = ""; this.checked = false; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; }
  appendChild(c) { c.parent = this; this.children.push(c); return c; }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  get firstChild() { return this.children[0] || null; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  every(pred, acc = []) { for (const c of this.children) { if (pred(c)) acc.push(c); c.every(pred, acc); } return acc; }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = { createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
globalThis.location = { hash: "#/admin/people" };
let copied = null;
Object.defineProperty(globalThis, "navigator", { value: { clipboard: { writeText: async (t) => { copied = t; } } }, configurable: true, writable: true });

const jsDir = process.argv[2];
const scenario = process.argv[3];
const settle = () => new Promise((r) => setTimeout(r, 15));
const KEY = "AbCdEfGhIjKlMnOpQrStUvWxYz0123456789-_abcde";

/* The session the scripted GET answers as; each scenario sets it before the first draw. */
const session = { can_decide: true, signed_in_admin: true, sign_in_key: KEY };
const puts = [];
globalThis.fetch = async (url, init) => {
  const u = String(url);
  const reply = (status, body) => ({ status, ok: status < 400, text: async () => JSON.stringify(body), headers: { get: () => "application/json" } });
  if (init && init.method === "PUT") {
    puts.push({ path: u, body: JSON.parse(init.body) });
    return reply(200, { status: "added", message: "Added." });
  }
  /* As GET /api/people answers (#7107): the list only to an administrator, and any other session its own key alone. */
  if (session.signed_in_admin !== true) return reply(200, { ...session });
  return reply(200, {
    ...session,
    people_returned: 1,
    people: [{
      person_id: 5, display_name: "Ann", enabled: true, updated_by: "person:alice", updated_at_utc: null,
      identities: [{ identity_id: 9, kind: "slack", external_id: "T1:U1" }],
      grants: [{ grant_id: 3, capability: "approve", kinds: ["kill_session"], scope: "server", scope_server_id: 1, scope_server_name: "SRV1" }],
    }],
  });
};

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "people-"));
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(path.join(scratch, "panels.js"),
    'import { el } from "./util.js";\n' +
    'export const VIZ = { table(data, opts) { const rows = data[opts.rowsKey]; const col = opts.columns.find((c) => c.key === "manage");\n' +
    '  return el("div", { class: "stub-table" }, rows.map((r) => el("div", { class: "stub-row" }, [el("span", { text: r.name + " | " + r.signins + " | " + r.grants }), col.render(r)]))); } };\n');
  fs.writeFileSync(path.join(scratch, "pages", "admin.js"), "export const unused = 1;\n");

  const mod = await import(pathToFileURL(path.join(scratch, "pages", "admin-people.js")).href);
  const body = new FakeNode("div");
  let generation = 0;
  const ctx = {
    isCurrent: (g) => g === generation,
    failure: (res) => (res.kind === "data" ? null : new FakeNode("div")),
    getSession: async () => ({ managed_store: false, admin_writes_off: false }),
    readScopes: async () => ({ servers: [{ id: 1, name: "SRV1" }], tags: [{ id: 7, name: "prod" }] }),
    repaint: () => { void mod.buildPeople(body, ++generation, ctx); },
  };
  const draw = async () => { ctx.repaint(); await settle(); };

  const buttons = () => body.every((n) => n.tag === "button");
  const button = (text) => buttons().find((b) => b.textContent === text || b.getAttribute("aria-label") === text);
  const click = async (b) => { if (b && !b.disabled) for (const fn of b.listeners.click || []) await fn({}); await settle(); };
  const fieldNode = (name) => body.every((n) => n.getAttribute("data-field") === name)[0];
  const type = (name, value, event = "input") => { const n = fieldNode(name); n.value = value; for (const fn of n.listeners[event] || []) fn({}); };
  const strips = () => body.every((n) => n.className.startsWith("strip")).map((n) => n.textContent);
  const text = () => body.textContent;

  const out = {};
  const scenarios = {
    /* A signed-in admin with a key: the key shows with a copy button, and "Add my own sign-in" sends exactly that key as a web identity. */
    async ownKey() {
      await draw();
      const key = fieldNode("sign-in-key");
      out.keyShown = key ? key.textContent : null;
      await click(button("Copy your sign-in key"));
      out.copied = copied;
      await click(button("Open Ann"));
      out.detailShown = body.every((n) => n.getAttribute("data-role") === "person-detail").length === 1;
      await click(button("Add my own sign-in"));
      out.puts = puts;
    },
    /* The shared web token (an admin with no key): no key shows, the page says why, and there is no "Add my own sign-in". */
    async sharedToken() {
      session.sign_in_key = null;
      await draw();
      out.keyShown = !!fieldNode("sign-in-key");
      out.noKeySentence = strips().some((s) => s.includes("The shared web token has none"));
      await click(button("Open Ann"));
      out.addOwnPresent = !!button("Add my own sign-in");
      out.addSignInPresent = !!button("Add sign-in");
    },
    /* A viewer sign-in: the route sends it no list, so the tab shows its own key and the Copy button, and nothing else. */
    async viewer() {
      session.can_decide = false;
      session.signed_in_admin = false;
      await draw();
      out.keyShown = fieldNode("sign-in-key") ? fieldNode("sign-in-key").textContent : null;
      out.listShown = text().includes("Ann");
      out.addPersonPresent = !!button("Add person");
      out.otherButtons = buttons().map((b) => b.textContent).filter((t) => t !== "Copy");
      out.copyPresent = !!button("Copy your sign-in key");
      out.strips = strips().length;
    },
    /* The admin's forms: add a person, a Slack sign-in (upper-cased, team and user joined) and an approve grant with its kinds on a tag. */
    async forms() {
      await draw();
      type("newName", "  Bob  ");
      await click(button("Add person"));
      await click(button("Open Ann"));
      type("kind", "slack", "change");
      await settle();
      type("team", "t9");
      type("user", "u7");
      await click(button("Add sign-in"));
      type("capability", "approve", "change");
      await settle();
      out.kindBoxes = body.every((n) => n.getAttribute("data-kind")).length;
      await click(button("Add grant"));
      out.noKindsRefused = strips().some((s) => s.includes("Pick at least one server action"));
      const box = body.every((n) => n.getAttribute("data-kind") === "evict_plan")[0];
      box.checked = true;
      for (const fn of box.listeners.change || []) fn({});
      type("scope", "t7", "change");
      await click(button("Add grant"));
      await click(button("Disable Ann"));
      out.puts = puts;
    },
  };
  await scenarios[scenario]();
  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
