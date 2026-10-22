/* Runs the shipped plan-row.js (wwwroot/js/plan-row.js, with util.js) on a small fake DOM and prints what a scenario did as
   one line of JSON. PlanRowBehaviourTests starts it as
       node web-plan-row-harness.mjs <path to the js folder> <scenario>
   Only the DOM, its geometry and ResizeObserver are stand-ins: the observer records what it watches and whether it is still
   connected, so a scenario can count the observers a panel leaves behind. */
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag) { this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = ""; this.className = ""; this.style = {}; this.rect = { left: 0, width: 0 }; this.clientWidth = 0; this.clientLeft = 0; }
  get isConnected() { let n = this; while (n.parent) n = n.parent; return n === body; }
  /* A tiny layout: a node with `h` set is that tall; a spacer is its style height; a panel is out of flow but has its own height;
     a row is its tallest cell; anything else stacks its in-flow children. */
  get offsetHeight() {
    if (this.className === "plan-panel") return this.h || 0;
    if (this.h !== undefined) return this.h;
    if (this.className === "plan-spacer") return parseFloat(this.style.height) || 0;
    const flow = this.children.filter((c) => c.className !== "plan-panel").map((c) => c.offsetHeight);
    return this.tag === "tr" ? Math.max(0, ...flow) : flow.reduce((a, b) => a + b, 0);
  }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  appendChild(c) { if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c); c.parent = this; this.children.push(c); return c; }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  removeEventListener(t, fn) { this.listeners[t] = (this.listeners[t] || []).filter((f) => f !== fn); }
  fire(t) { for (const fn of this.listeners[t] || []) fn({ target: this }); }
  getBoundingClientRect() { return { left: this.rect.left, width: this.rect.width, right: this.rect.left + this.rect.width }; }
  /* Only the two selectors plan-row.js asks for: a tag name and a class. */
  closest(sel) { for (let n = this; n; n = n.parent) if (sel.startsWith(".") ? n.className.split(" ").includes(sel.slice(1)) : n.tag === sel) return n; return null; }
}
class FakeObserver {
  static all = [];
  constructor(cb) { this.cb = cb; this.targets = []; this.live = true; FakeObserver.all.push(this); }
  observe(t) { this.targets.push(t); this.seen = null; }
  disconnect() { this.live = false; this.targets = []; }
  fire() { this.cb([]); }
  /* Like the real thing: calls back only when something it watches changed size since its last call. */
  poll() {
    if (!this.live) return false;
    const sig = this.targets.map((t) => t.offsetHeight).join(",");
    if (sig === this.seen) return false;
    this.seen = sig; this.cb([]); return true;
  }
}
const body = new FakeNode("body");
globalThis.Node = FakeNode;
globalThis.document = { body, createElement: (t) => new FakeNode(t), createTextNode: (t) => Object.assign(new FakeNode("#text"), { _text: t }) };
globalThis.ResizeObserver = FakeObserver;

const planRow = await import(pathToFileURL(process.argv[2] + "/plan-row.js").href);
const out = {};
const live = () => FakeObserver.all.filter((o) => o.live).length;
const scrollListeners = (wrap) => (wrap.listeners.scroll || []).length;

/* wrap > table > tbody > tr > td > host(button), on the page. */
function grid(rows = 2) {
  const wrap = new FakeNode("div"); wrap.className = "table-wrap"; wrap.clientWidth = 600;
  const table = new FakeNode("table"); const tbody = new FakeNode("tbody");
  wrap.appendChild(table); table.appendChild(tbody); body.appendChild(wrap);
  const hosts = [];
  for (let i = 0; i < rows; i++) {
    const tr = new FakeNode("tr"); const td = new FakeNode("td"); const host = new FakeNode("div"); host.className = planRow.CLOSED_CLASS;
    host.appendChild(new FakeNode("button")); td.appendChild(host); tr.appendChild(td); tbody.appendChild(tr); hosts.push(host);
  }
  return { wrap, tbody, hosts };
}
const panelNode = (h) => { const p = new FakeNode("div"); p.className = "plan-panel"; if (h !== undefined) p.h = h; return p; };

/* A row as the Queries grid draws it: a tall cell of its own text, a History cell and a Plan cell (each a button), on the page. */
function historyAndPlanRow(ownHeight = 60) {
  const wrap = new FakeNode("div"); wrap.className = "table-wrap"; wrap.clientWidth = 600;
  const table = new FakeNode("table"); const tbody = new FakeNode("tbody"); const tr = new FakeNode("tr");
  wrap.appendChild(table); table.appendChild(tbody); tbody.appendChild(tr); body.appendChild(wrap);
  const text = new FakeNode("td"); text.h = ownHeight; tr.appendChild(text);
  const hosts = ["history", "plan"].map(() => {
    const td = new FakeNode("td"); const host = new FakeNode("div"); host.className = planRow.CLOSED_CLASS;
    const button = new FakeNode("button"); button.h = 20; host.appendChild(button); td.appendChild(host); tr.appendChild(td); return host;
  });
  return { tr, hosts };
}
/* Lets every observer react to size changes until none has anything left to say, or `cap` rounds pass (a row that never settles). */
function settle(cap = 12) {
  let rounds = 0;
  while (rounds < cap) {
    let any = false;
    for (const o of FakeObserver.all) any = o.poll() || any;
    if (!any) break;
    rounds++;
  }
  return rounds;
}

const scenarios = {
  /* The pure rule: where the panel sits so it covers the wrap's visible area. */
  async geometry() {
    const g = planRow.pinGeometry;
    out.scrolledRight = g({ wrapLeft: 0, wrapClientLeft: 0, wrapClientWidth: 600, rowLeft: -2487, rowWidth: 3087 });
    out.notScrolled = g({ wrapLeft: 0, wrapClientLeft: 0, wrapClientWidth: 600, rowLeft: 0, rowWidth: 3087 });
    out.narrowTable = g({ wrapLeft: 0, wrapClientLeft: 0, wrapClientWidth: 600, rowLeft: 0, rowWidth: 400 });
    out.offsetWrap = g({ wrapLeft: 250, wrapClientLeft: 1, wrapClientWidth: 600, rowLeft: -1000, rowWidth: 3087 });
    out.pastTheEnd = g({ wrapLeft: 0, wrapClientLeft: 0, wrapClientWidth: 600, rowLeft: -9000, rowWidth: 3087 });
    out.beforeTheStart = g({ wrapLeft: 0, wrapClientLeft: 0, wrapClientWidth: 600, rowLeft: 80, rowWidth: 3087 });
  },
  /* The panel is pinned on the first fit, follows a scroll of the wrap, and follows a resize of the wrap. */
  async pinsToVisibleArea() {
    const { wrap, hosts } = grid();
    const host = hosts[0]; const row = host.closest("tr"); const panel = panelNode();
    row.rect = { left: -2487, width: 3087 };
    planRow.dockPanelUnderRow(host, panel);
    FakeObserver.all[0].fire();
    out.afterOpen = { left: panel.style.left, width: panel.style.width, right: panel.style.right };
    row.rect = { left: -1000, width: 3087 }; wrap.fire("scroll");
    out.afterScroll = { left: panel.style.left, width: panel.style.width };
    wrap.clientWidth = 450; FakeObserver.all[0].fire();
    out.afterResize = { left: panel.style.left, width: panel.style.width };
    out.watching = FakeObserver.all[0].targets.map((t) => t.tag + ":" + t.className);
  },
  /* History and Plan open on one row (#6359): the row settles at its own height plus both panels, the panels stacked. */
  async twoPanelsOneRow() {
    const { tr, hosts } = historyAndPlanRow(60);
    const [history, plan] = hosts; const hp = panelNode(100); const pp = panelNode(150);
    out.closedRow = tr.offsetHeight;
    planRow.dockPanelUnderRow(history, hp); out.historyAlone = (settle(), tr.offsetHeight);
    out.historyAloneBottom = hp.style.bottom;
    planRow.dockPanelUnderRow(plan, pp);
    out.rounds = settle();
    out.both = tr.offsetHeight;
    out.historyBottom = hp.style.bottom; out.planBottom = pp.style.bottom;
    /* A later pass (a resize, a reload) leaves it where it is. */
    for (let i = 0; i < 3; i++) for (const o of FakeObserver.all) o.fire();
    out.afterMore = tr.offsetHeight;
    /* The Plan panel loads taller. */
    pp.h = 220; settle(); out.planGrew = tr.offsetHeight; out.historyBottomAfterGrow = hp.style.bottom;
    /* Closing Plan puts the row back to History alone. */
    plan.children = plan.children.filter((c) => c.tag === "button"); planRow.undockPanel(plan); settle();
    out.afterPlanClosed = tr.offsetHeight; out.historyBottomAfterClose = hp.style.bottom;
  },
  /* One panel alone sizes as it always has: the row's own height plus the panel. */
  async onePanelAlone() {
    const { tr, hosts } = historyAndPlanRow(60);
    const p = panelNode(100);
    planRow.dockPanelUnderRow(hosts[1], p);
    out.rounds = settle(); out.height = tr.offsetHeight; out.bottom = p.style.bottom;
  },
  /* One observer per open panel; closing, redrawing and discarding the grid all let go of it. */
  async observerLifecycle() {
    const { wrap, tbody, hosts } = grid();
    const [a, b] = hosts;
    out.start = live();
    planRow.dockPanelUnderRow(a, panelNode());
    out.afterOpen = live();
    FakeObserver.all[0].fire();
    out.scrollListenersOpen = scrollListeners(wrap);
    planRow.undockPanel(a);
    out.afterClose = live();
    out.scrollListenersClosed = scrollListeners(wrap);
    out.closedClass = a.className;
    /* Drawn again and again (a panel reloading) never stacks observers. */
    for (let i = 0; i < 4; i++) { planRow.dockPanelUnderRow(a, panelNode()); FakeObserver.all[FakeObserver.all.length - 1].fire(); }
    out.afterRedraws = live();
    out.scrollListenersRedrawn = scrollListeners(wrap);
    /* A grid re-render takes the row out of the page; the next time the observer fires the panel is let go. */
    planRow.dockPanelUnderRow(b, panelNode());
    FakeObserver.all[FakeObserver.all.length - 1].fire();
    out.beforeRerender = live();
    tbody.removeChild(b.closest("tr"));
    for (const o of FakeObserver.all.filter((x) => x.live)) o.fire();
    out.afterRerender = live();
    out.scrollListenersAfterRerender = scrollListeners(wrap);
  },
};

await scenarios[process.argv[3]]();
console.log(JSON.stringify(out));
