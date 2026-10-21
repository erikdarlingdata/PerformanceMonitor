/* The Block chain action on the shipped Blocking row (#6292). Runs the shipped wwwroot/js/pages/block-chain.js with the real
   util.js on a small fake DOM and a recording fetch, and prints as one line of JSON what the page asked for and drew.
   BlockChainWebTests starts it as
       node block-chain-harness.mjs <path to wwwroot/js> <scenario>
   Only the DOM and fetch are stand-ins. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag) {
    this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = "";
    this.className = ""; this.dataset = {}; this.value = ""; this.open = false;
  }
  get parentNode() { return this.parent; }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this.children.forEach((c) => (c.parent = null)); this.children = []; this._text = String(v); }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; }
  appendChild(c) { if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c); c.parent = this; this.children.push(c); return c; }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  fire(t) { for (const fn of this.listeners[t] || []) fn({ preventDefault() {} }); }
  all(tag, acc = []) { for (const c of this.children) { if (c.tag === tag) acc.push(c); c.all(tag, acc); } return acc; }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = { createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
globalThis.location = { hash: "#/server" };

const jsDir = process.argv[2];
const scenario = process.argv[3];

const urls = [];
let reply = () => ({ status: 200, body: {} });
globalThis.fetch = async (url) => {
  const u = String(url);
  urls.push(u);
  const r = reply(Object.fromEntries(new URL(u, "http://x").searchParams));
  return { status: r.status, ok: r.status < 300, text: async () => JSON.stringify(r.body) };
};
const readsOf = () => urls.filter((u) => u.includes("/get_block_chain")).map((u) => Object.fromEntries(new URL(u, "http://x").searchParams));

const node = (spid, over = {}, children = []) => ({
  spid, ecid: 0, is_apex: false, is_apex_sleeping: false, wait_time_ms: 2000, lock_mode: "X", database_name: "SalesDb",
  login_name: "svc", host_name: "web1", client_app: "app", contentious_object: null, tran_started: null,
  sql_text: "UPDATE t SET c = 1", sql_text_truncated: false, children, ...over,
});
const HOSTILE = "<img src=x onerror=alert(1)>";
const chainAnswer = () => ({
  status: "ok", server: "srv", event_time: "2026-01-01T12:00:00.0000000Z", session_id: 58, session_ecid: 0,
  cycle_detected: false, depth_capped: false, traversal_truncated: false,
  chain: node(52, { is_apex: true, wait_time_ms: 0, login_name: HOSTILE, sql_text: "SELECT '" + HOSTILE + "'" },
    [node(55, { lock_mode: "X" }, [node(58, { wait_time_ms: 3000, host_name: "<script>x</script>" })])]),
});

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "block-chain-"));
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const { blockChainCell } = await import(pathToFileURL(path.join(scratch, "pages", "block-chain.js")).href);

  const tick = () => new Promise((r) => setTimeout(r, 30));
  const walk = (n, acc = []) => { acc.push(n); n.children.forEach((c) => walk(c, acc)); return acc; };
  const row = { event_time: "2026-01-01T12:00:00.0000000", blocked_spid: 58, blocked_ecid: 0, blocking_spid: 55 };
  const button = (cell) => cell.all("button")[0];
  /* The tree as the DOM holds it: each li's title and the titles of the li's directly under it. */
  const shape = (cell) => walk(cell).filter((n) => n.tag === "li").map((li) => ({
    title: li.children[0].textContent,
    children: li.children.filter((c) => c.tag === "ul").flatMap((ul) => ul.children.map((c) => c.children[0].textContent)),
  }));

  const out = {};
  const scenarios = {
    tree: async () => {
      reply = () => ({ status: 200, body: chainAnswer() });
      const cell = blockChainCell("srv", row);
      out.before = { label: button(cell).textContent, shape: shape(cell) };
      button(cell).fire("click"); await tick();
      out.reads = readsOf();
      out.shape = shape(cell);
      out.label = button(cell).textContent;
      const all = walk(cell);
      /* Collected text is data: no element was made from it, and the hostile text is on the page as plain text. */
      out.injectedElements = all.filter((n) => n.tag === "img" || n.tag === "script").length;
      out.text = cell.textContent;
      out.hostileLoginAsText = all.some((n) => n.tag === "dd" && n.textContent === HOSTILE);
      /* A second click hides it, a third shows it again without a second read. */
      button(cell).fire("click");
      out.hiddenShape = shape(cell);
      button(cell).fire("click"); await tick();
      out.shownAgainShape = shape(cell);
      out.readsAfterToggle = readsOf().length;
    },
    empty: async () => {
      reply = () => ({ status: 200, body: { status: "empty", message: "No blocking chain could be rebuilt for session 58 near this report." } });
      const cell = blockChainCell("srv", row);
      button(cell).fire("click"); await tick();
      out.text = cell.textContent;
      out.nodes = shape(cell).length;
    },
    failed: async () => {
      reply = () => ({ status: 500, body: { error: "read failed" } });
      const cell = blockChainCell("srv", row);
      button(cell).fire("click"); await tick();
      out.text = cell.textContent;
      out.nodes = shape(cell).length;
    },
    /* The grid redraws every row when it refreshes (once a minute): the same row comes back as a NEW cell. */
    refresh: async () => {
      reply = () => ({ status: 200, body: chainAnswer() });
      const first = blockChainCell("srv", row);
      button(first).fire("click"); await tick();
      const again = blockChainCell("srv", { ...row });
      out.afterRedraw = { label: button(again).textContent, shape: shape(again) };
      /* Closing and opening the redrawn cell still does not read again. */
      button(again).fire("click"); button(again).fire("click"); await tick();
      out.afterToggle = shape(again).length;
      /* A redraw while the first read is still in flight joins it instead of reading again. */
      const inFlightRow = { ...row, blocked_spid: 71 };
      const slow = blockChainCell("srv", inFlightRow);
      button(slow).fire("click");
      const joined = blockChainCell("srv", { ...inFlightRow });
      await tick();
      out.joinedShape = shape(joined).length;
      out.readsForInFlight = readsOf().filter((q) => q.blocked_spid === "71").length;
      /* A row that is not the same row (another session) is its own closed cell and reads for itself. */
      const other = blockChainCell("srv", { ...row, blocked_spid: 99 });
      out.otherLabel = button(other).textContent;
      out.otherShape = shape(other).length;
      out.reads = readsOf().length;
    },
    nokey: async () => {
      out.cellText = blockChainCell("srv", { blocked_spid: 58 }).textContent;
      out.buttons = blockChainCell("srv", { blocked_spid: 58 }).all("button").length;
    },
  };
  await scenarios[scenario]();
  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
