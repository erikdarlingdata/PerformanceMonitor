/* #7565: runs the shipped view-save-warnings.js (the strip the view editors show when a save comes back with the validator's
   warnings) and prints as one line of JSON what it returned. ViewSaveWarningsBehaviourTests starts it as
       node view-save-warnings-harness.mjs <path to wwwroot/js>
   The whole js tree is copied into a scratch folder first (#5279). Only the DOM is a stand-in. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[2];

class FakeNode {
  constructor(tag, text) {
    this.style = {};
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this._text = text == null ? "" : String(text);
    this.className = "";
  }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  appendChild(c) { this.children.push(c); return c; }
  addEventListener() {}
}
globalThis.Node = FakeNode;
globalThis.document = {
  body: new FakeNode("body"),
  querySelectorAll: () => [],
  createElement: (t) => new FakeNode(t),
  createTextNode: (t) => new FakeNode("#text", t),
  createDocumentFragment: () => new FakeNode("#fragment"),
  addEventListener() {},
  removeEventListener() {},
};
globalThis.location = { hash: "" };

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "view-save-warnings-"));
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const m = await import(pathToFileURL(path.join(scratch, "view-save-warnings.js")).href);
  const strip = m.buildSaveWarningsStrip(["panel 0: use perfmon_value_per_second <b>x</b>", "cell 2: second"], "#/view/9");
  const link = strip.children[strip.children.length - 1];
  console.log(JSON.stringify({
    none: m.savedViewWarnings({ id: 1 }),
    nullView: m.savedViewWarnings(null),
    mixed: m.savedViewWarnings({ warnings: ["a", "  ", 5, "b"] }),
    stripClass: strip.className,
    lines: strip.children.filter((c) => c.tag === "div").map((c) => c.textContent),
    linkTag: link.tag,
    linkHref: link.attrs.href,
  }));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
