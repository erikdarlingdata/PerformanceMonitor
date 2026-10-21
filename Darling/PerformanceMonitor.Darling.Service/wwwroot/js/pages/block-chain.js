/* The Block chain action on a blocked-process row (#6292): the lead blocker at the root and the sessions it blocks under it, as
   the desktop viewer's Block Chain window shows them. The service builds the tree (get_block_chain, the viewer's own
   reconstruction); this file only draws what it returns. Every collected value reaches the page as text. */

import { dockPanelUnderRow, undockPanel } from "../plan-row.js";
import { el, readTool, loadingStrip, errorStrip, emptyStrip, noticeStrip, fmtMs, localTime } from "../util.js";

const NONE_TEXT = "No blocking chain could be rebuilt for this session near this report.";

/** The label line of one node: the SPID (with its ECID when it is not 0) and what the session is in the chain. */
function nodeTitle(n) {
  const id = "SPID " + n.spid + (Number(n.ecid) > 0 ? " (ECID " + n.ecid + ")" : "");
  if (n.is_apex) return id + (n.is_apex_sleeping ? " - lead blocker, sleeping" : " - lead blocker");
  return id;
}

/** The fields a node shows, as [label, text] pairs in a fixed order; a field the report did not carry is left out. */
function nodeFields(n) {
  const fields = [];
  if (!n.is_apex && n.wait_time_ms != null) fields.push(["Waited", fmtMs(n.wait_time_ms)]);
  if (n.lock_mode) fields.push(["Lock mode", n.lock_mode]);
  if (n.database_name) fields.push(["Database", n.database_name]);
  if (n.contentious_object) fields.push(["Object", n.contentious_object]);
  if (n.login_name) fields.push(["Login", n.login_name]);
  if (n.host_name) fields.push(["Host", n.host_name]);
  if (n.client_app) fields.push(["App", n.client_app]);
  if (n.tran_started) fields.push(["Transaction started", localTime(n.tran_started)]);
  return fields;
}

function drawNode(n) {
  const item = el("li", { class: "block-chain-node" + (n.is_apex ? " block-chain-apex" : "") });
  item.appendChild(el("div", { class: "block-chain-title", text: nodeTitle(n) }));
  const fields = nodeFields(n);
  if (fields.length > 0) {
    const list = el("dl", { class: "block-chain-fields" });
    for (const [label, text] of fields) {
      list.appendChild(el("dt", { text: label }));
      list.appendChild(el("dd", { text: String(text) }));
    }
    item.appendChild(list);
  }
  if (n.sql_text) {
    const details = el("details", { class: "block-chain-sql" });
    details.appendChild(el("summary", { text: n.sql_text_truncated ? "SQL (cut)" : "SQL" }));
    details.appendChild(el("pre", { text: n.sql_text }));
    item.appendChild(details);
  }
  const children = Array.isArray(n.children) ? n.children : [];
  if (children.length > 0) {
    const sub = el("ul", { class: "block-chain-children" });
    for (const child of children) sub.appendChild(drawNode(child));
    item.appendChild(sub);
  }
  return item;
}

/** The tree for one get_block_chain answer, as a `.block-chain` div: a notice for each bound the reconstruction hit, then the
 *  tree. */
export function drawBlockChain(answer) {
  const host = el("div", { class: "block-chain" });
  if (answer.cycle_detected) host.appendChild(noticeStrip("The blockers form a cycle here, so the tree is cut where it loops back."));
  if (answer.depth_capped) host.appendChild(noticeStrip("The chain is deeper than the tree draws, so its lowest levels are left off."));
  if (answer.traversal_truncated) host.appendChild(noticeStrip("The chain is too large to follow completely, so the tree is partial."));
  const root = el("ul", { class: "block-chain-tree" });
  root.appendChild(drawNode(answer.chain));
  host.appendChild(root);
  return host;
}

/* What each opened row holds, by the row's own identity (server, event time, blocked session), so a grid that draws again
   (the page's refresh redraws every row) finds its chain still open and already read instead of closing it or reading it a
   second time. `body` is the one panel element: the read fills it in, whichever cell currently holds it. Oldest entries are
   dropped past MAX_ENTRIES so a long session does not keep every chain it ever opened. */
const entries = new Map();
const MAX_ENTRIES = 50;

function entryFor(key) {
  let entry = entries.get(key);
  if (!entry) {
    entry = { open: false, loaded: false, pending: false, body: el("div", { class: "block-chain-body plan-panel" }) };
    entries.set(key, entry);
    if (entries.size > MAX_ENTRIES) entries.delete(entries.keys().next().value);
  }
  return entry;
}

/** Reads the chain into the entry's panel (once, until it has loaded). */
async function load(server, row, entry) {
  if (entry.loaded || entry.pending) return;
  entry.pending = true;
  const body = entry.body;
  body.textContent = "";
  body.appendChild(loadingStrip("Reading the block chain…"));
  let res;
  try {
    res = await readTool("get_block_chain", {
      server,
      event_time: row.event_time,
      blocked_spid: row.blocked_spid,
      blocked_ecid: row.blocked_ecid == null ? 0 : row.blocked_ecid,
    });
  } catch (e) {
    res = { kind: "error", message: String((e && e.message) || e) };
  }
  entry.pending = false;
  body.textContent = "";
  if (res.kind === "data" && res.data && res.data.chain) {
    entry.loaded = true;
    body.appendChild(drawBlockChain(res.data));
  } else if (res.kind === "empty" || (res.kind === "data" && res.data && res.data.status === "empty")) {
    entry.loaded = true;
    body.appendChild(emptyStrip((res.data && res.data.message) || res.message || NONE_TEXT));
  } else {
    body.appendChild(errorStrip((res && res.message) || "The block chain could not be read."));
  }
}

/** The Block chain cell of one Blocking row: a button that reads the chain once and shows it in a panel under the row (the
 *  stored-plan panel's placement, js/plan-row.js, not inside the last column), and hides it again. A row without its event
 *  time or blocked session has nothing to key the read on, so it is a dash. */
export function blockChainCell(server, row) {
  if (!row || row.event_time == null || row.event_time === "" || row.blocked_spid == null) return document.createTextNode("—");
  const ecid = row.blocked_ecid == null ? 0 : row.blocked_ecid;
  const entry = entryFor([server, row.event_time, row.blocked_spid, ecid].join("|"));
  const host = el("div", { class: "plan-cell" });
  const draw = () => {
    while (host.firstChild) host.removeChild(host.firstChild);
    const button = el("button", {
      type: "button",
      class: "grid-tool",
      text: entry.open ? "Hide block chain" : "Block chain",
      "aria-expanded": entry.open ? "true" : "false",
      title: "Show the lead blocker and the sessions it blocks, for this report",
    });
    button.addEventListener("click", () => {
      entry.open = !entry.open;
      draw();
      if (entry.open) load(server, row, entry);
    });
    host.appendChild(button);
    if (entry.open) dockPanelUnderRow(host, entry.body);
    else undockPanel(host);
  };
  draw();
  /* A row drawn again while its chain is open and not yet read (a refresh landed mid-read) joins the read in flight; one that
     is open and read just shows it. Neither starts a second read. */
  if (entry.open) load(server, row, entry);
  return host;
}

/** The column that carries the cell, in the shape the Blocking grid's other action columns use. */
export function blockChainColumn(server) {
  return {
    key: "block_chain_action",
    label: "Block chain",
    render: (row) => blockChainCell(server, row),
    sortable: false,
    filter: false,
    csv: false,
    copy: false,
  };
}
