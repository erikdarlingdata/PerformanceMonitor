/* Runs the shipped proposal page (wwwroot/js/pages/proposals.js) and the route page's nobody-can-approve strips (pages/admin-agent-setup.js)
   with the real util.js on a small fake DOM and a scripted fetch, and prints as one line of JSON what a person would see and what the page
   sent (#7109).
   WebProposalsBehaviourTests starts it as
       node web-proposals-harness.mjs <path to wwwroot/js> <scenario>
   Stand-ins: the DOM, fetch, and panels.js. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag) { this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = ""; this.className = ""; this.dataset = {}; this.disabled = false; this.style = {}; }
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
globalThis.location = { hash: "#/proposals" };

const jsDir = process.argv[2];
const scenario = process.argv[3];
const settle = () => new Promise((r) => setTimeout(r, 15));
const ID = "0b9a3f4e-1111-4222-8333-444455556666";

/* What the scripted GET answers; each scenario sets these before the first draw. */
const seat = { can_press: true, presses_as: "person", person_name: "Ann", press_note: null };
const proposal = {
  proposal_id: ID, request_id: "aa000000-0000-4000-8000-000000000001", kind: "kill_session", tool: "kill_session", agent: "agent-a",
  server_id: 7, server_name: "pg-7", state: "pending", target: { session_id: 87 },
  binding: { pid: 87, state: "idle in transaction", application_name: "<img src=x onerror=alert(1)>" },
  agent_reason: "<b>blocks</b> everyone", proposed_at_utc: "2026-11-01T10:00:00Z", expires_at_utc: "2026-11-01T11:00:00Z",
  first_press_by: null, first_press_at_utc: null, first_press_facts: null, decided_by: null, outcome: null,
};
let pressAnswer = { status: 200, body: { status: "first_pressed", code: null, message: "These are the facts now.", facts: { pid: 87 }, changed: [] } };
const posts = [];
globalThis.fetch = async (url, init) => {
  const u = String(url);
  const reply = (status, body) => ({ status, ok: status < 400, text: async () => JSON.stringify(body), headers: { get: () => "application/json" } });
  if (init && init.method === "POST") {
    posts.push({ path: u, body: JSON.parse(init.body) });
    if (u.endsWith("/first-press") && pressAnswer.body.status === "first_pressed") {
      proposal.first_press_by = "person:5";
      proposal.first_press_at_utc = "2026-11-01T10:05:00Z";
    }
    return reply(pressAnswer.status, pressAnswer.body);
  }
  if (u === "/api/proposals") return reply(200, { ...seat, proposals_returned: 1, proposals: [proposal] });
  if (u === "/api/proposals/" + ID) return reply(200, { ...seat, proposal });
  return reply(404, { error: "no such route" });
};

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "proposals-"));
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(path.join(scratch, "panels.js"), "export const VIZ = {};\n");

  const mod = await import(pathToFileURL(path.join(scratch, "pages", "proposals.js")).href);
  const body = new FakeNode("div");
  const draw = async (id) => { await mod.renderProposals(body, id); await settle(); };
  const buttons = () => body.every((n) => n.tag === "button");
  const button = (action) => buttons().find((b) => b.getAttribute("data-action") === action);
  const click = async (b) => { if (b && !b.disabled) for (const fn of b.listeners.click || []) await fn({}); await settle(); };
  const role = (r) => body.every((n) => n.getAttribute("data-role") === r)[0];
  const strips = () => body.every((n) => n.className.startsWith("strip")).map((n) => ({ kind: n.className, text: n.textContent }));
  const imgNodes = () => body.every((n) => n.tag === "img" || n.tag === "b").length;

  const out = {};
  const scenarios = {
    /* A mapped person: the facts, the agent's reason labelled as the agent's and drawn as text, the request link, then the first press and
       the confirm, each a POST of an empty object. */
    async person() {
      await draw(ID);
      out.reason = role("agent-reason") ? role("agent-reason").textContent : null;
      out.markupNodes = imgNodes();
      out.binding = role("binding") ? role("binding").textContent : null;
      const request = role("request");
      out.requestLink = request ? request.every((n) => n.tag === "a")[0].getAttribute("href") : null;
      out.buttonsBefore = buttons().map((b) => b.getAttribute("data-action"));
      await click(button("first-press"));
      out.afterFirst = strips().map((s) => s.text);
      out.pressFacts = role("press-facts") ? role("press-facts").textContent : null;
      out.buttonsAfter = buttons().map((b) => b.getAttribute("data-action"));
      pressAnswer = { status: 200, body: { status: "run", code: null, message: "The action ran.", facts: { pid: 87 }, changed: [] } };
      await click(button("confirm"));
      out.posts = posts;
    },
    /* A superseded press: the message as an error, and what changed. */
    async superseded() {
      pressAnswer = { status: 409, body: { status: "superseded", code: "binding_changed", message: "The facts changed since the agent proposed this. Nothing was run.", facts: { pid: 87, state: "active" }, changed: ["state", "state_change"] } };
      await draw(ID);
      await click(button("first-press"));
      out.strips = strips();
      out.changed = role("changed") ? role("changed").textContent : null;
    },
    /* An administrator path: the note that says why a two-person route needs a mapped person shows before any press. */
    async administrator() {
      Object.assign(seat, { presses_as: "administrator", person_name: null, press_note: "Your sign-in is not mapped to an enabled person, so you press as an administrator. A route that needs two people takes a press only from a mapped person." });
      await draw(ID);
      out.strips = strips().map((s) => s.text);
      out.buttons = buttons().map((b) => b.getAttribute("data-action"));
    },
    /* A session that cannot press: the proposal reads, the note shows, and there is no button. */
    async readOnly() {
      Object.assign(seat, { can_press: false, presses_as: null, person_name: null, press_note: "Sign in to the web as an admin to press a proposal." });
      await draw(ID);
      out.state = role("state") ? role("state").textContent : null;
      out.strips = strips().map((s) => s.text);
      out.buttons = buttons().length;
      await draw("");
      out.listRows = body.every((n) => n.getAttribute("data-role") === "proposal-row").length;
      out.listLink = body.every((n) => n.tag === "a").map((a) => a.getAttribute("href"))[0];
    },
    /* The route page: one strip for each route the check names, and none for a failed read. */
    async routeSentence() {
      const setup = await import(pathToFileURL(path.join(scratch, "pages", "admin-agent-setup.js")).href);
      out.strips = setup.nobodyCanApproveNodes({ kind: "data", data: { route_ids: [4, 9], message: "Nobody can approve this. Map a person, or turn on single-person approval." } }).map((n) => n.textContent);
      out.failed = setup.nobodyCanApproveNodes({ kind: "error", message: "no" }).length;
    },
  };
  await scenarios[scenario]();
  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
