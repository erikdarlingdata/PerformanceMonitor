/* Runs the shipped Custom Views folder pieces (#6214) under Node against a fake DOM and a stubbed localStorage, and prints the
   result of a scenario as one line of JSON. CustomViewFoldersWebTests starts it as
       node view-folders-harness.mjs <path to wwwroot/js> <scenario>
   The sidebar list (view-sidebar.js), the grouping and access rules (view-folders.js), the editors' folder / personal / owner
   fields (view-meta-fields.js) and the per-browser collapse state (viewer-local.js) are copied unchanged into a scratch folder
   and imported as the browser would. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [jsDir, scenario] = process.argv.slice(-2);

class FakeNode {
  constructor(tag, text) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.style = {};
    this.className = "";
    this.value = "";
    this.checked = false;
    this.disabled = false;
    this.handlers = {};
    this.text = text == null ? null : String(text);
  }
  get firstChild() { return this.children[0] || null; }
  appendChild(child) { this.children.push(child); return child; }
  removeChild(child) {
    const i = this.children.indexOf(child);
    if (i >= 0) this.children.splice(i, 1);
    return child;
  }
  setAttribute(name, value) { this.attrs[name] = String(value); }
  focus() { globalThis.document.activeElement = this; }
  addEventListener(type, fn) { (this.handlers[type] ||= []).push(fn); }
  set textContent(value) { this.children = []; this.text = String(value); }
  get textContent() { return (this.text || "") + this.children.map((c) => c.textContent).join(""); }
}

globalThis.Node = FakeNode;
globalThis.document = {
  activeElement: null,
  createElement: (tag) => new FakeNode(tag),
  createTextNode: (text) => new FakeNode("#text", text),
};
globalThis.window = globalThis;
globalThis.location = { hash: "", pathname: "/", search: "", href: "http://localhost/", origin: "http://localhost" };
const data = {};
globalThis.localStorage = {
  getItem: (k) => (Object.prototype.hasOwnProperty.call(data, k) ? data[k] : null),
  setItem: (k, v) => { data[k] = String(v); },
  removeItem: (k) => { delete data[k]; },
};

const all = (node, pred, out = []) => {
  if (pred(node)) out.push(node);
  for (const c of node.children) all(c, pred, out);
  return out;
};
const fire = (node, type, e = {}) => { for (const fn of node.handlers[type] || []) fn(e); };

const v = (id, name, o = {}) => ({ id, name, kind: "dashboard", version: 1, folder: null, personal: false, created_by: "alice", updated_by: "alice", ...o });
const views = [
  v(1, "Zeta top level"),
  v(2, "Alpha top level"),
  v(3, "CPU", { folder: "Ops" }),
  v(4, "Waits", { folder: "Ops" }),
  v(5, "Blocking", { folder: "Dev", kind: "notebook" }),
  v(6, "Old team view", { created_by: null, updated_by: null }),
  v(7, "My scratch", { personal: true, folder: "Ignored", created_by: "alice" }),
  v(8, "Another scratch", { personal: true, created_by: "alice" }),
  v(9, "Bob private", { personal: true, created_by: "bob" }),
  v(10, "apple", { folder: "ops" }),
];
const shape = (rows) => rows.map((r) => (r.kind === "group" ? "G:" + r.name + "(" + r.count + (r.collapsed ? ",c" : "") + ")" : "V:" + r.view.name + "@" + r.depth));

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "view-folders-"));
const out = {};
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  const rules = await load("view-folders.js");
  const side = await load("view-sidebar.js");
  const fields = await load("view-meta-fields.js");
  const local = await load("viewer-local.js");
  const none = () => false;

  if (scenario === "order") {
    out.alice = shape(rules.sidebarViewRows(views, "alice", none));
    out.bob = shape(rules.sidebarViewRows(views, "bob", none));
    out.noIdentity = shape(rules.sidebarViewRows(views, null, none));
    out.aliceOpsCollapsed = shape(rules.sidebarViewRows(views, "alice", (id) => id === "folder:Ops"));
    out.aliceMyCollapsed = shape(rules.sidebarViewRows(views, "alice", (id) => id === "my"));
    out.onlyTopLevel = shape(rules.sidebarViewRows([views[0], views[1]], "alice", none));
    out.empty = shape(rules.sidebarViewRows([], "alice", none));
    out.folders = rules.folderNames(views);
  } else if (scenario === "collapse") {
    local.setStorage(globalThis.localStorage);
    local.toggleViewGroup("folder:Ops");
    local.toggleViewGroup("my");
    out.storedAfterTwo = JSON.parse(data["darling.local.viewGroups.v1"]);
    out.opsCollapsed = local.isViewGroupCollapsed("folder:Ops");
    out.devCollapsed = local.isViewGroupCollapsed("folder:Dev");
    // a rebuilt page reads the same state back
    local.reload();
    out.opsAfterReload = local.isViewGroupCollapsed("folder:Ops");
    local.toggleViewGroup("folder:Ops");
    out.opsAfterToggleBack = local.isViewGroupCollapsed("folder:Ops");
    // ids that are not groups are ignored, never stored
    local.toggleViewGroup("tag:3");
    local.toggleViewGroup("folder:");
    local.toggleViewGroup(7);
    local.toggleViewGroup("folder:" + "x".repeat(101));
    local.toggleViewGroup("folder:bad\u0007name");
    out.storedAfterJunk = JSON.parse(data["darling.local.viewGroups.v1"]);
    // a hand-edited value is cleaned on load, and the server-tag groups' own key is untouched
    data["darling.local.viewGroups.v1"] = JSON.stringify({ v: 1, collapsed: ["my", "folder:Ok", 5, "nope", "folder:" + "y".repeat(200)] });
    local.reload();
    out.cleaned = ["my", "folder:Ok", "nope"].map((id) => local.isViewGroupCollapsed(id));
    out.serverGroupsKey = data["darling.local.sidebar.v1"] === undefined;
  } else if (scenario === "paint") {
    const list = new FakeNode("div");
    local.setStorage(globalThis.localStorage);
    const repaint = () => side.paintViewList(list, views, { can_edit: true, subject: "alice" }, 4);
    local.onChange(repaint);
    repaint();
    const lines = () => list.children.map((n) => (n.dataset.viewGroup !== undefined ? "G:" + n.children[1].textContent + n.children[2].textContent + n.attrs["aria-expanded"] : "V:" + n.textContent + (n.className.includes("active") ? "*" : "") + (n.attrs.href || "")));
    out.first = lines();
    const ops = all(list, (n) => n.dataset.viewGroup === "folder:Ops")[0];
    fire(ops, "click");
    out.afterCollapse = lines();
    out.stored = JSON.parse(data["darling.local.viewGroups.v1"]);
    out.headerCount = all(list, (n) => n.dataset.viewGroup !== undefined).length;
    // the shared-token session: no My views, and no New links when it cannot edit
    side.paintViewList(list, [views[0], views[6]], { can_edit: false, subject: null }, null);
    out.tokenReadOnly = lines();
    const empty = new FakeNode("div");
    side.paintViewList(empty, [], { can_edit: false, subject: null }, null);
    out.emptyText = empty.textContent;
    // a name that looks like markup stays text
    const hostile = new FakeNode("div");
    side.paintViewList(hostile, [v(20, "<img src=x onerror=alert(1)>", { folder: "<b>f</b>" })], { can_edit: false }, null);
    out.hostile = hostile.children.map((n) => n.textContent);
  } else if (scenario === "fields") {
    const session = (subject) => ({ can_edit: true, subject });
    const build = (view, subject, meta) => fields.buildViewMetaFields({ meta: meta || { folder: view ? view.folder || "" : "", personal: view ? view.personal === true : false }, view, session: session(subject), views, onChange: () => { out.changed = (out.changed || 0) + 1; } });
    const inspect = (b) => {
      const inputs = all(b.node, (n) => n.tag === "input");
      const folder = inputs.find((n) => n.attrs.type === "text");
      const box = inputs.find((n) => n.attrs.type === "checkbox");
      return {
        folderValue: folder.value, folderDisabled: folder.disabled, personalChecked: box.checked, personalDisabled: box.disabled,
        readOnly: b.readOnly, text: b.node.textContent,
        options: all(b.node, (n) => n.tag === "option").map((n) => n.attrs.value),
        listLinked: folder.attrs.list === all(b.node, (n) => n.tag === "datalist")[0].attrs.id,
        maxlength: folder.attrs.maxlength,
      };
    };
    out.newWithIdentity = inspect(build(null, "alice"));
    out.newNoIdentity = inspect(build(null, null));
    out.ownShared = inspect(build(views[2], "alice"));
    out.otherShared = inspect(build(views[2], "bob"));
    out.legacyShared = inspect(build(views[5], "alice"));
    out.ownPersonal = inspect(build(views[6], "alice"));
    out.othersPersonal = inspect(build(views[8], "alice"));
    out.othersPersonalNoIdentity = inspect(build(views[8], null));

    // ticking Personal locks the folder box and the model follows; the editor is told
    const meta = { folder: "Ops", personal: false };
    const b = build(null, "alice", meta);
    const inputs = all(b.node, (n) => n.tag === "input");
    const folder = inputs.find((n) => n.attrs.type === "text");
    const box = inputs.find((n) => n.attrs.type === "checkbox");
    folder.value = "Dev";
    fire(folder, "input");
    box.checked = true;
    fire(box, "change");
    out.afterTicking = { meta: { ...meta }, folderDisabled: folder.disabled, text: b.node.textContent, changed: out.changed };
    box.checked = false;
    fire(box, "change");
    out.afterUnticking = { meta: { ...meta }, folderDisabled: folder.disabled };

    // what the save body carries
    out.save = {
      typed: rules.metaForSave({ folder: "  Ops  ", personal: false }, null, session("alice")),
      blank: rules.metaForSave({ folder: "   ", personal: false }, null, session("alice")),
      personal: rules.metaForSave({ folder: "Ops", personal: true }, null, session("alice")),
      personalNoIdentity: rules.metaForSave({ folder: "", personal: true }, null, session(null)),
      personalLegacy: rules.metaForSave({ folder: "", personal: true }, views[5], session("alice")),
      personalOthers: rules.metaForSave({ folder: "", personal: true }, views[2], session("bob")),
      ownPersonalStays: rules.metaForSave({ folder: "", personal: true }, views[6], session("alice")),
    };
    out.owner = [rules.ownerLine(views[2]), rules.ownerLine(views[5]), rules.ownerLine({ id: 1, created_by: "a", updated_by: "b" }), rules.ownerLine(null)];
  } else if (scenario === "access") {
    const note = (view, subject) => rules.viewAccess(view, { subject });
    out.shared = note(views[0], "alice");
    out.own = note(views[6], "alice");
    out.others = note(views[8], "alice");
    out.noIdentity = note(views[8], null);
    out.legacyPersonal = note(v(30, "x", { personal: true, created_by: null }), "alice");
  } else {
    throw new Error("unknown scenario " + scenario);
  }
  process.stdout.write(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
