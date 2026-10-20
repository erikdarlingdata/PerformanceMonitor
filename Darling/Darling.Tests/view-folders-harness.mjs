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

const v = (id, name, o = {}) => ({ id, name, kind: "dashboard", version: 1, folder: null, personal: false, created_by: "alice", updated_by: "alice", mine: false, ...o });
/* The server marks `mine` (the owner key matches); the harness uses the created_by name as the stand-in for that key. */
const as = (list, user) => list.map((x) => ({ ...x, mine: user !== null && x.created_by === user }));
const asOne = (view, user) => (view ? as([view], user)[0] : view);
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
    out.alice = shape(rules.sidebarViewRows(as(views, "alice"), none));
    out.bob = shape(rules.sidebarViewRows(as(views, "bob"), none));
    out.noIdentity = shape(rules.sidebarViewRows(as(views, null), none));
    out.aliceOpsCollapsed = shape(rules.sidebarViewRows(as(views, "alice"), (id) => id === "folder:Ops"));
    out.aliceMyCollapsed = shape(rules.sidebarViewRows(as(views, "alice"), (id) => id === "my"));
    out.onlyTopLevel = shape(rules.sidebarViewRows([views[0], views[1]], none));
    out.empty = shape(rules.sidebarViewRows([], none));
    out.folders = rules.folderNames(views);
    // the page never compares names: a personal view that carries my display name but is not `mine` is not in My views
    out.sameNameNotMine = shape(rules.sidebarViewRows([v(40, "Impostor", { personal: true, created_by: "alice", mine: false })], none));
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
    const repaint = () => side.paintViewList(list, as(views, "alice"), { can_edit: true, can_own: true }, 4);
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
    side.paintViewList(list, as([views[0], views[6]], null), { can_edit: false, can_own: false }, null);
    out.tokenReadOnly = lines();
    const empty = new FakeNode("div");
    side.paintViewList(empty, [], { can_edit: false, can_own: false }, null);
    out.emptyText = empty.textContent;
    // a name that looks like markup stays text
    const hostile = new FakeNode("div");
    side.paintViewList(hostile, [v(20, "<img src=x onerror=alert(1)>", { folder: "<b>f</b>" })], { can_edit: false }, null);
    out.hostile = hostile.children.map((n) => n.textContent);
  } else if (scenario === "fields") {
    const session = (user) => ({ can_edit: true, can_own: user !== null });
    const build = (view, subject, meta) => fields.buildViewMetaFields({ meta: meta || { folder: view ? view.folder || "" : "", personal: view ? view.personal === true : false }, view: asOne(view, subject), session: session(subject), views, onChange: () => { out.changed = (out.changed || 0) + 1; } });
    const inspect = (b) => {
      const inputs = all(b.node, (n) => n.tag === "input");
      const folder = inputs.find((n) => n.attrs.type === "text");
      const box = inputs.find((n) => n.attrs.type === "checkbox");
      return {
        folderValue: folder.value, folderDisabled: folder.disabled, personalChecked: box.checked, personalDisabled: box.disabled,
        readOnly: b.readOnly, blocked: b.blocked, text: b.node.textContent,
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
      personalLegacy: rules.metaForSave({ folder: "", personal: true }, asOne(views[5], "alice"), session("alice")),
      personalOthers: rules.metaForSave({ folder: "", personal: true }, asOne(views[2], "bob"), session("bob")),
      ownPersonalStays: rules.metaForSave({ folder: "", personal: true }, asOne(views[6], "alice"), session("alice")),
    };
    // a folder name the store would refuse: the editor says so and blocks Save; a personal view has no folder to refuse
    const badMeta = { folder: "Dev\u202Es", personal: false };
    const bad = build(null, "alice", badMeta);
    const badBox = all(bad.node, (n) => n.tag === "input").find((n) => n.attrs.type === "checkbox");
    const badFolder = all(bad.node, (n) => n.tag === "input").find((n) => n.attrs.type === "text");
    const blockedFor = (text) => { badFolder.value = text; fire(badFolder, "input"); return bad.blocked; };
    out.folderBlock = {
      hidden: blockedFor("Dev\u202Es"), zeroWidth: blockedFor("Dev\u200Bs"), reserved: blockedFor("my VIEWS"), tooLong: blockedFor("x".repeat(101)),
      fine: blockedFor("Devs"), note: (() => { blockedFor("My views"); return bad.node.textContent; })(),
      personalIgnoresFolder: (() => { badBox.checked = true; fire(badBox, "change"); return bad.blocked; })(),
    };
    out.folderProblems = ["", "  ", "Ops", "My views", "MY VIEWS", " my views ", "a\u202Eb", "a\u200Bb", "a\tb", "x".repeat(101), "x".repeat(100)].map((n) => rules.folderProblem(n) !== null);
    out.owner = [rules.ownerLine(views[2]), rules.ownerLine(views[5]), rules.ownerLine({ id: 1, created_by: "a", updated_by: "b" }), rules.ownerLine(null)];
  } else if (scenario === "access") {
    const note = (view, user) => rules.viewAccess(asOne(view, user), { can_edit: true, can_own: user !== null });
    out.shared = note(views[0], "alice");
    out.own = note(views[6], "alice");
    out.others = note(views[8], "alice");
    out.noIdentity = note(views[8], null);
    out.legacyPersonal = note(v(30, "x", { personal: true, created_by: null }), "alice");
  } else if (scenario === "signout") {
    local.setStorage(globalThis.localStorage);
    local.toggleViewGroup("folder:Secret plans");
    out.before = local.isViewGroupCollapsed("folder:Secret plans");
    local.clearViewGroups();
    out.after = local.isViewGroupCollapsed("folder:Secret plans");
    out.keyGone = data["darling.local.viewGroups.v1"] === undefined;
    local.reload();
    out.afterReload = local.isViewGroupCollapsed("folder:Secret plans");
  } else if (scenario === "expired") {
    // The shipped app.js pieces (the view list, the fleet sidebar and the signed-out takeover), run against stand-ins: once the
    // session is expired, nothing repaints the old session's lists, and the cached list is dropped.
    const vm = await import("node:vm");
    const full = fs.readFileSync(path.join(jsDir, "app.js"), "utf8").replace(/\r\n/g, "\n");
    const slice = (from, to) => {
      const a = full.indexOf(from);
      const b = full.indexOf(to, a);
      if (a < 0 || b < a) throw new Error("app.js layout changed: " + from);
      return full.slice(a, b);
    };
    const source = [
      slice("/* The last good fleet read, kept at module scope", "/* ─────────────────────────── availability-groups nav gate"),
      slice("async function refreshViewList()", "/* ─────────────────────────── status bar"),
      slice("function showSignedOutState(", "/* ─────────────────────────── auto-refresh play/pause"),
    ].join("\n");
    const calls = { paintViewList: [], paintServerList: 0, mounts: 0, cleared: 0 };
    let expired = false;
    let release = null;
    const ctx = vm.createContext({
      console, Promise, Array, location: { pathname: "/", search: "", hash: "" },
      viewList: {}, serverList: {}, statusbar: {}, main: {},
      el: () => ({}), mount: () => { calls.mounts++; },
      isSessionExpired: () => expired,
      getSession: async () => ({ can_edit: true, can_own: true }),
      listViews: () => (release ? new Promise((r) => { release = () => r({ kind: "data", data: as(views, "alice") }); }) : Promise.resolve({ kind: "data", data: as(views, "alice") })),
      currentRoute: () => ({ name: "home", id: null, param: null }),
      isViewItemRoute: () => false,
      paintViewList: (_c, list) => { calls.paintViewList.push(list.filter((x) => x.personal && x.mine).length); },
      paintServerList: () => { calls.paintServerList++; },
      updateStatusBar: () => {},
      clearViewGroups: () => { calls.cleared++; },
    });
    vm.runInContext(source + "\nthis.api = { refreshViewList, paintViewListNow, paintSidebar, showSignedOutState, setFleet: (x) => { lastSidebarFleet = x; }, last: () => lastViewList };", ctx);
    await ctx.api.refreshViewList();
    ctx.api.setFleet({ data: {} });
    ctx.api.paintSidebar();
    out.beforeExpiry = { views: [...calls.paintViewList], servers: calls.paintServerList, cached: ctx.api.last() !== null };
    expired = true;
    ctx.api.showSignedOutState("Your session is no longer valid.");
    ctx.api.paintViewListNow();
    ctx.api.paintSidebar();
    out.afterExpiry = { views: [...calls.paintViewList], servers: calls.paintServerList, cached: ctx.api.last() !== null, cleared: calls.cleared };
    // a list read that lands after the session ended keeps nothing and paints nothing
    release = () => {};
    const inFlight = ctx.api.refreshViewList();
    release();
    await inFlight;
    out.lateRead = { views: [...calls.paintViewList], cached: ctx.api.last() !== null };
  } else {
    throw new Error("unknown scenario " + scenario);
  }
  process.stdout.write(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
