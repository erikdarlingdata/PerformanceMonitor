/* Drives the web time range picker (wwwroot/js/time-range-picker.js, #5562) in a small stand-in DOM and prints what it did as one
   line of JSON. WebTimeRangePickerTests starts it as
       node time-range-picker-harness.mjs <path to wwwroot/js>
   The js tree is copied into a scratch folder (so util.js's `el` builder, the picker's one import beside time-range.js, loads as an
   ES module) and imported. The DOM is a node tree of plain objects that can fire events up through their parents; the clock and the
   zone are the picker's own options, so the run does not depend on the machine's. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[process.argv.length - 1];

class FakeEl {
  constructor(tag, text) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.style = {};
    this.listeners = {};
    this.parentNode = null;
    this.className = "";
    this.value = "";
    this.disabled = false;
    this.text = text == null ? null : String(text);
  }
  get firstChild() {
    return this.children[0] || null;
  }
  appendChild(child) {
    child.parentNode = this;
    this.children.push(child);
    return child;
  }
  removeChild(child) {
    const i = this.children.indexOf(child);
    if (i >= 0) this.children.splice(i, 1);
    child.parentNode = null;
    return child;
  }
  contains(other) {
    for (let n = other; n; n = n.parentNode) if (n === this) return true;
    return false;
  }
  setAttribute(name, value) {
    this.attrs[name] = String(value);
  }
  getAttribute(name) {
    return name in this.attrs ? this.attrs[name] : null;
  }
  addEventListener(type, fn) {
    (this.listeners[type] ||= []).push(fn);
  }
  focus() {
    document.activeElement = this;
  }
  set textContent(value) {
    this.children = [];
    this.text = String(value);
  }
  get textContent() {
    return (this.text || "") + this.children.map((c) => c.textContent).join("");
  }
}

const documentListeners = {};
globalThis.Node = FakeEl;
globalThis.document = {
  activeElement: null,
  createElement: (tag) => new FakeEl(tag),
  createElementNS: (ns, tag) => new FakeEl(tag),
  createTextNode: (text) => new FakeEl("#text", text),
  addEventListener: (type, fn) => (documentListeners[type] ||= []).push(fn),
  removeEventListener: (type, fn) => {
    const list = documentListeners[type] || [];
    const i = list.indexOf(fn);
    if (i >= 0) list.splice(i, 1);
  },
};

/* Fire an event at a node and let it bubble up through its parents, unless a listener stops it. */
function fire(target, type, extra = {}) {
  let stopped = false;
  const event = { type, target, key: extra.key, preventDefault() {}, stopPropagation() { stopped = true; }, ...extra };
  for (let n = target; n && !stopped; n = n.parentNode) for (const fn of n.listeners[type] || []) fn(event);
  return event;
}
const walkAll = (root, test, out = []) => {
  if (test(root)) out.push(root);
  root.children.forEach((c) => walkAll(c, test, out));
  return out;
};
const byClass = (root, cls) => walkAll(root, (n) => (n.className || "").split(" ").includes(cls));
const text = (node) => (node ? node.textContent : null);

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "time-range-picker-"));
let picker;
let tr;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  picker = await import(pathToFileURL(path.join(scratch, "time-range-picker.js")).href);
  tr = await import(pathToFileURL(path.join(scratch, "time-range.js")).href);
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const NOW = Date.parse("2026-10-08T11:01:00Z");
const ZONE = "America/New_York";
const make = (extra = {}) => {
  documentListeners.mousedown = [];
  const changes = [];
  const p = picker.timeRangePicker({ zone: ZONE, now: () => NOW, onChange: (spec, range) => changes.push({ id: tr.specId(spec), label: range.label }), ...extra });
  const root = new FakeEl("div");
  root.appendChild(p.node);
  return { p, root, changes, button: byClass(p.node, "trp-button")[0] };
};
const items = (root) => byClass(root, "trp-item").map((b) => ({ name: text(byClass(b, "trp-item-name")[0]), length: text(byClass(b, "trp-item-length")[0]), why: text(byClass(b, "trp-item-why")[0]), disabled: b.attrs["aria-disabled"] === "true", pressed: b.attrs["aria-pressed"] }));
const itemNamed = (root, name) => byClass(root, "trp-item").find((b) => text(byClass(b, "trp-item-name")[0]) === name);
const typeInto = (root, value) => {
  const box = byClass(root, "trp-text")[0];
  box.value = value;
  fire(box, "input");
  return { preview: text(byClass(root, "trp-preview")[0]), previewClass: byClass(root, "trp-preview")[0].className, applyDisabled: byClass(root, "btn")[0].disabled };
};

const found = {};

/* 1. The closed picker: the button names the range, the detail has the resolved start, end and zone. */
{
  const { p, button } = make();
  const detail = byClass(p.node, "trp-detail")[0];
  found.closed = { button: text(button), detail: text(detail), ariaLabel: button.attrs["aria-label"], expanded: button.attrs["aria-expanded"], haspopup: button.attrs["aria-haspopup"], windowHours: p.window().hours, windowAsOf: p.window().asOf };
}

/* 2. Opening: presets, calendar periods with their current lengths, the reach greying the long ones out with the reason. */
{
  const { p, root, button } = make();
  fire(button, "click");
  const popup = byClass(root, "trp-popup")[0];
  found.open = {
    disabled: items(root).filter((i) => i.disabled).map((i) => i.name + ' | ' + i.why),
    expanded: button.attrs["aria-expanded"],
    role: popup.attrs.role,
    dialogLabel: popup.attrs["aria-label"],
    items: items(root),
    textLabel: byClass(root, "trp-text")[0].attrs["aria-label"],
    pickLabels: byClass(root, "trp-pick").map((b) => b.attrs["aria-label"]),
    documentMousedownListeners: (documentListeners.mousedown || []).length,
    focusedText: document.activeElement === byClass(root, "trp-text")[0],
    from: byClass(root, "trp-pick")[0].value,
    to: byClass(root, "trp-pick")[1].value,
  };
  void p;
}

/* 3. Typing shows the parsed range before Apply; Enter applies it. */
{
  const { p, root, changes, button } = make();
  fire(button, "click");
  found.typedOk = typeInto(root, "45m");
  const box = byClass(root, "trp-text")[0];
  fire(box, "keydown", { key: "Enter" });
  found.typedApplied = { changes, button: text(button), open: p.isOpen(), expanded: button.attrs["aria-expanded"], docListeners: (documentListeners.mousedown || []).length, window: p.window() };
}

/* 4. Refusals are shown and Enter does nothing: under 5 minutes, past the reach, nonsense. */
{
  const { p, root, changes, button } = make();
  fire(button, "click");
  found.tooShort = typeInto(root, "3m");
  fire(byClass(root, "trp-text")[0], "keydown", { key: "Enter" });
  found.tooLong = typeInto(root, "last month");
  fire(byClass(root, "trp-text")[0], "keydown", { key: "Enter" });
  found.nonsense = typeInto(root, "banana");
  found.empty = typeInto(root, "");
  found.refusedChanges = changes.length;
  found.stillOpen = p.isOpen();
}

/* 5. Esc closes and returns focus to the button, from the text box (the key bubbles) and from the button. */
{
  const { p, root, button } = make();
  fire(button, "click");
  fire(byClass(root, "trp-text")[0], "keydown", { key: "Escape" });
  found.escape = { open: p.isOpen(), focusOnButton: document.activeElement === button, expanded: button.attrs["aria-expanded"], popups: byClass(root, "trp-popup").length, docListeners: (documentListeners.mousedown || []).length };
}

/* 6. A press outside closes the popup without raising a change; one inside does not. */
{
  const { p, root, changes, button } = make();
  fire(button, "click");
  const inside = (documentListeners.mousedown || [])[0];
  inside({ target: byClass(root, "trp-text")[0] });
  const stillOpen = p.isOpen();
  inside({ target: new FakeEl("div") });
  found.outside = { stillOpenAfterInside: stillOpen, openAfterOutside: p.isOpen(), changes: changes.length, docListeners: (documentListeners.mousedown || []).length };
}

/* 7. A preset applies at once; a greyed-out one does nothing; a calendar period holds its spec. */
{
  const { p, root, changes, button } = make();
  fire(button, "click");
  fire(itemNamed(root, "Past 15 minutes"), "click");
  const afterPreset = { changes: [...changes], button: text(button), open: p.isOpen() };
  fire(button, "click");
  fire(itemNamed(root, "Past 30 days"), "click");
  const afterDisabled = { changes: changes.length, open: p.isOpen() };
  fire(itemNamed(root, "Previous Week"), "click");
  found.presets = { afterPreset, afterDisabled, afterPeriod: { changes: [...changes], button: text(button), detail: text(byClass(p.node, "trp-detail")[0]), window: p.window() } };
}

/* 8. The date and time boxes write the typed form and preview it. */
{
  const { root, button } = make();
  fire(button, "click");
  const [from, to] = byClass(root, "trp-pick");
  from.value = "2026-10-01T13:00";
  fire(from, "change");
  const afterOne = byClass(root, "trp-text")[0].value;
  to.value = "2026-10-01T15:00";
  fire(to, "change");
  found.picked = { afterOne, text: byClass(root, "trp-text")[0].value, preview: text(byClass(root, "trp-preview")[0]) };
}

/* 9. Compact hides the detail (it moves to the tooltip); the sample-interval and data-start notes appear; select() refuses what is not allowed. */
{
  const compact = make({ compact: true });
  const detail = byClass(compact.p.node, "trp-detail")[0];
  found.compact = { detail: text(detail), title: compact.p.node.attrs.title };
  const shortOne = make({ spec: tr.relativeSpec(5 * 60000), sampleIntervalMs: 5 * 60000 });
  const wide = make({ spec: tr.relativeSpec(4 * 3600000), sampleIntervalMs: 5 * 60000, dataStartMs: NOW - 2 * 3600000 });
  found.notes = { short: text(byClass(shortOne.p.node, "trp-note")[0]), wide: text(byClass(wide.p.node, "trp-note")[0]) };
  const m = make();
  found.select = { tooLong: m.p.select(tr.relativeSpec(30 * 86400000)), tooShort: m.p.select(tr.relativeSpec(60000)), ok: m.p.select(tr.relativeSpec(3600000)), changes: m.changes.map((c) => c.id) };
}

/* 10. The reach comes from a catalog entry's max_hours when it carries one; a picker with that reach offers the long periods. */
{
  const entry = { name: "get_x", params: [{ name: "hours", type: "int", max_hours: 2160 }] };
  const reach = tr.reachHours(entry);
  const { root, button } = make({ reachHours: reach });
  fire(button, "click");
  found.reach = { reach, none: tr.reachHours({ name: "y", params: [] }), absent: tr.reachHours(null), items: items(root).filter((i) => i.disabled).map((i) => i.name) };
}

/* 11. A live range slides: refresh() redraws at the clock it is given. setSpec reads an id back. */
{
  let clock = NOW;
  const { p, button } = make({ now: () => clock, spec: tr.relativeSpec(3600000) });
  const first = text(byClass(p.node, "trp-detail")[0]);
  clock = NOW + 3600000;
  p.refresh();
  const second = text(byClass(p.node, "trp-detail")[0]);
  p.setSpec("previous-month");
  found.slides = { first, second, afterSetSpec: text(button), specId: tr.specId(p.spec()) };
}

console.log(JSON.stringify(found));
