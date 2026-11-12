/* Runs the web panel layout (#7534): panel-layout.js against a stand-in DOM, and prints which panels were marked to take the
   whole row as one line of JSON. WebPanelLayoutBehaviourTests starts it as
       node web-panel-layout-harness.mjs <path to wwwroot/js> */
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[process.argv.length - 1];

class FakeNode {
  constructor(name, cls) {
    this.name = name;
    this.nodeType = 1;
    this.children = [];
    this.parentNode = null;
    this.style = {};
    this.hidden = false;
    this.className = cls || "";
    const self = this;
    this.classList = {
      toggle(c, force) {
        const set = new Set(self.className.split(/\s+/).filter(Boolean));
        if (force) set.add(c);
        else set.delete(c);
        self.className = [...set].join(" ");
      },
    };
  }
  add(child) {
    child.parentNode = this;
    this.children.push(child);
    return child;
  }
}

const { soloPanels, keepLonePanelsWide } = await import(pathToFileURL(path.join(jsDir, "panel-layout.js")).href);

const panel = (name, span2) => new FakeNode(name, "panel card" + (span2 ? " span-2" : ""));
const solo = (grid) => {
  const out = [];
  const walk = (n) => n.children.forEach((c) => ((" " + c.className + " ").includes(" span-solo ") && out.push(c.name), walk(c)));
  walk(grid);
  return out;
};
const grid = (...kids) => {
  const g = new FakeNode("grid", "panel-grid");
  kids.forEach((k) => g.add(k));
  return g;
};
const out = {};

/* A span-1 panel before a full-row panel is alone. */
let g = grid(panel("a"), panel("wide", true));
soloPanels(g);
out.loneBeforeWide = solo(g);

/* Two span-1 panels share a row and neither is marked. */
g = grid(panel("a"), panel("b"), panel("wide", true));
soloPanels(g);
out.pair = solo(g);

/* Three in a row: the third is alone. And a span-1 panel at the end of the page is alone. */
g = grid(panel("a"), panel("b"), panel("c"), panel("wide", true), panel("d"));
soloPanels(g);
out.three = solo(g);

/* A partner that hides itself leaves the other alone, and shows again. */
const b = panel("b");
g = grid(panel("a"), b, panel("wide", true));
soloPanels(g);
const before = solo(g);
b.hidden = true;
b.style.display = "none";
soloPanels(g);
out.hiddenPartner = { before, after: solo(g) };
b.hidden = false;
b.style.display = "";
soloPanels(g);
out.shownAgain = solo(g);

/* A panel inside a display:contents slot counts in the grid, and the mark moves off a panel that gains a partner. */
const slot = new FakeNode("slot", "");
slot.style.display = "contents";
slot.add(panel("inner"));
g = grid(slot, panel("wide", true));
soloPanels(g);
const alone = solo(g);
slot.add(panel("inner2"));
soloPanels(g);
out.slot = { alone, paired: solo(g) };

/* A full-row sentence with nothing in it does not split a pair; a filled one does. */
const intro = new FakeNode("intro", "panel-intro");
g = grid(panel("a"), intro, panel("b"));
soloPanels(g);
const emptyIntro = solo(g);
intro.add(new FakeNode("text", ""));
soloPanels(g);
out.intro = { empty: emptyIntro, filled: solo(g) };

/* #7534: an item that takes the whole row with an inline grid-column ends a run too, like a span-2 panel. */
const inlineRow = (name, gc) => {
  const n = new FakeNode(name, "");
  n.style.gridColumn = gc;
  return n;
};
g = grid(panel("a"), inlineRow("bar", "1 / -1"), panel("b"), panel("c"), inlineRow("holder", "1 / span 2"), panel("d"));
soloPanels(g);
out.inlineRow = solo(g);

/* The observer runs the same walk when a panel is added. */
let callback = null;
globalThis.MutationObserver = class {
  constructor(cb) { callback = cb; }
  observe() {}
  disconnect() {}
};
g = grid(panel("a"));
keepLonePanelsWide(g);
const first = solo(g);
const second = g.add(panel("b"));
callback([{ type: "childList", target: g, addedNodes: [second], removedNodes: [] }]);
out.observed = { first, after: solo(g) };

console.log(JSON.stringify(out));
