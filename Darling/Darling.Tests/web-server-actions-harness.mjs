/* Runs the shipped Admin "Server actions" tab (wwwroot/js/pages/admin-server-actions.js) with the real util.js on a small fake DOM and a
   scripted fetch, and prints as one line of JSON what a person would see while a switch change is in flight (#6310).
   WebServerActionsBehaviourTests starts it as
       node web-server-actions-harness.mjs <path to wwwroot/js> <scenario>
   Stand-ins: the DOM, fetch, panels.js (its table only draws each row's Change cell) and pages/admin.js. A click on a disabled
   button is dropped by the stand-in `click`, as a browser drops it; a click on an enabled one is delivered. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag) { this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = ""; this.className = ""; this.dataset = {}; this.disabled = false; }
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
globalThis.location = { hash: "#/admin/server-actions" };

const jsDir = process.argv[2];
const scenario = process.argv[3];
const settle = () => new Promise((r) => setTimeout(r, 15));

/* The store: two switches on one server. PUTs wait on a gate the scenario opens, so a change stays in flight. */
const store = { a: true, b: true };
const puts = [];
const gates = [];
globalThis.fetch = async (url, init) => {
  const u = String(url);
  const reply = (status, body) => ({ status, ok: status < 400, text: async () => JSON.stringify(body), headers: { get: () => "application/json" } });
  if (init && init.method === "PUT") {
    const key = decodeURIComponent(u.split("/").pop());
    puts.push(key);
    await new Promise((r) => gates.push(r));
    const enabled = JSON.parse(init.body).enabled;
    store[key] = enabled;
    return reply(200, { status: "set", message: "Switch " + key + " is " + (enabled ? "on" : "off") + "." });
  }
  const actions = (k) => ({ action: k, valid: true, enabled: store[k], set_by: "admin", updated_at_utc: null });
  return reply(200, { can_decide: true, servers: [{ server_id: 1, server_name: "SRV1", engine_kind: "sqlserver", actions: [actions("a"), actions("b")] }] });
};

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "server-actions-"));
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(path.join(scratch, "panels.js"),
    'import { el } from "./util.js";\n' +
    'export const VIZ = { table(data, opts) { const rows = data[opts.rowsKey]; const col = opts.columns.find((c) => c.key === "change");\n' +
    '  return el("div", { class: "stub-table" }, rows.map((r) => el("div", { class: "stub-row", "data-key": r.action_key }, col ? [col.render(r)] : []))); } };\n');
  fs.writeFileSync(path.join(scratch, "pages", "admin.js"), "export const unused = 1;\n");

  const mod = await import(pathToFileURL(path.join(scratch, "pages", "admin-server-actions.js")).href);
  const body = new FakeNode("div");
  let generation = 0;
  const ctx = {
    isCurrent: (g) => g === generation,
    failure: (res) => (res.kind === "data" ? null : new FakeNode("div")),
    repaint: () => { void mod.buildServerActions(body, ++generation, ctx); },
  };
  ctx.repaint();
  await settle();

  const buttons = () => body.every((n) => n.tag === "button");
  const button = (label) => buttons().find((b) => b.getAttribute("aria-label") === label);
  /* The browser: a click reaches a button only while it is enabled. Reports whether it was delivered. */
  const click = (b) => { if (b.disabled) return false; for (const fn of b.listeners.click || []) fn({}); return true; };
  const strips = () => body.every((n) => n.className.startsWith("strip")).map((n) => n.textContent);
  const release = async () => { gates.splice(0).forEach((g) => g()); await settle(); };

  const out = {};
  const scenarios = {
    /* A change is in flight and the person clicks the other switch: the click must not be taken and dropped. */
    async clickWhileSaving() {
      click(button("Turn off a on SRV1"));
      await settle();
      out.putsAfterFirst = puts.length;
      out.disabledWhileSaving = buttons().every((b) => b.disabled);
      out.savingLine = strips().some((s) => s.includes(mod.SAVING_SWITCH));
      out.secondClickDelivered = click(button("Turn off b on SRV1"));
      await settle();
      out.putsAfterSecond = puts.length;
      await release();
      out.enabledAfter = buttons().every((b) => !b.disabled);
      out.savingLineAfter = strips().some((s) => s.includes(mod.SAVING_SWITCH));
      out.answerShown = strips().some((s) => s.includes("Switch a is off"));
      /* After the answer the same click goes through. */
      click(button("Turn off b on SRV1"));
      await settle();
      out.putsAfterRetry = puts.length;
      await release();
    },
    /* "Turn on..." opens a confirm panel; a change started meanwhile locks that panel's buttons too. */
    async confirmWhileSaving() {
      store.b = false;
      ctx.repaint();
      await settle();
      click(button("Turn on b on SRV1"));
      await settle();
      out.confirmShown = body.every((n) => n.getAttribute("role") === "alertdialog").length === 1;
      const turnOn = buttons().find((b) => b.textContent === "Turn it on");
      out.turnItOnPresent = !!turnOn;
      click(button("Turn off a on SRV1"));
      await settle();
      out.putsAfterOff = puts.length;
      out.turnItOnDelivered = turnOn ? click(turnOn) : null;
      await settle();
      out.putsAfterConfirm = puts.length;
      await release();
    },
  };
  /* The Agent changes tab's Kind words (#6310): the kill row reads "kill session", as the Darling Viewer words it. */
  scenarios.agentKinds = async () => {
    const page = await import(pathToFileURL(path.join(scratch, "pages", "admin-agent-changes.js")).href);
    const kinds = ["plan_force", "plan_unforce", "plan_evict", "session_kill", "something_new"];
    out.kinds = page.agentChangeRows({ changes: kinds.map((kind, i) => ({ called_id: i, kind })) }).map((r) => r.kind);
  };
  await scenarios[scenario]();
  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
