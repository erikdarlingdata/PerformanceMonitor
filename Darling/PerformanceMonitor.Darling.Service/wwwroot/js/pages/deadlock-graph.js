/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * #5246: the deadlock graph in the Blocking tab's Deadlock Graphs grid. get_deadlock_detail sends each row's `graph`
 * already laid out by the shared C# parser and layout (process cards with x/y, waits-for edges with lock modes, the
 * independent cycles), so this file only draws it. The browser never reads the deadlock XML.
 *
 * Every value is drawn as text (textContent / el(..., {text})). The statement text is whatever the monitored server
 * captured, so none of it is ever parsed as markup.
 */

import { el, disclosure } from "../util.js";

/* The W3C namespace identifier createElementNS needs. It is never fetched, and the self-containment test allows
   it. charts.js keeps the same constant; it is repeated here so this file does not import the chart code. */
const SVG_NS = "http://www.w3.org/2000/svg";

function svg(tag, attrs) {
  const node = document.createElementNS(SVG_NS, tag);
  if (attrs) for (const [k, v] of Object.entries(attrs)) if (v != null) node.setAttribute(k, String(v));
  return node;
}

function svgText(attrs, text) {
  const node = svg("text", attrs);
  node.textContent = text;
  return node;
}

/* The curve is bowed this far to the right of its direction of travel, so the two arrows of a 2-cycle separate. */
const BOW = 38;
const LOOP_RX = 20;
const LOOP_RY = 16;
const CYCLE_PAD = 22;
const SQL_PREVIEW = 40;
let markerSeq = 0;

/* Which deadlocks have their graph open, at MODULE scope so the 60 s rebuild of the tab keeps it open. */
const deadlockGraphOpen = new Set();

function graphKey(server, row) {
  return server + "\u0001" + (row.dedup_key || (row.collection_time || "") + "|" + (row.deadlock_time || ""));
}

/* The cycles worth framing: a component of two or more processes, as the desktop viewer does. */
function realCycles(graph) {
  return (Array.isArray(graph.cycles) ? graph.cycles : []).filter((c) => num(c.node_count) >= 2);
}

/* "PAGE db.schema.table" -> "table" for the canvas; the tooltip keeps the whole label. */
function shortResource(label) {
  const s = String(label || "");
  const sp = s.indexOf(" ");
  if (sp > 0 && sp < s.length - 1) {
    const rest = s.slice(sp + 1);
    const dot = rest.lastIndexOf(".");
    if (dot >= 0 && dot < rest.length - 1) return rest.slice(dot + 1);
  }
  return s;
}

/** The Graph cell of one get_deadlock_detail row. Draws only when the reader opens it. */
export function deadlockGraphCell(server, row) {
  const graph = row.graph;
  if (!graph) {
    if (row.deadlock_graph_xml_truncated === true) {
      return document.createTextNode("The graph needs the full XML; this read sent a preview.");
    }
    return document.createTextNode("—");
  }
  if (graph.too_large) {
    const n = Number(graph.process_count) || 0;
    return document.createTextNode(n + " processes — too many to draw here; use Save XML or the desktop viewer.");
  }

  const processes = Array.isArray(graph.processes) ? graph.processes : [];
  const cycles = realCycles(graph);
  const body = el("div", { class: "dlg-host" });
  const summary =
    "Graph (" + processes.length + (processes.length === 1 ? " process" : " processes") +
    (cycles.length > 1 ? ", " + cycles.length + " cycles" : "") + ")";
  const node = disclosure(summary, body);
  const key = graphKey(server, row);

  let drawn = false;
  const draw = () => {
    if (drawn) return;
    drawn = true;
    body.appendChild(drawDeadlockGraph(graph, row));
  };
  node.addEventListener("toggle", () => {
    if (node.open) {
      deadlockGraphOpen.add(key);
      draw();
    } else {
      deadlockGraphOpen.delete(key);
    }
  });
  if (deadlockGraphOpen.has(key)) {
    node.setAttribute("open", "");
    draw();
  }
  return node;
}

/* Where the ray from a card's centre toward another point leaves the card rectangle. */
function clipToRect(cx, cy, tx, ty, halfW, halfH) {
  const dx = tx - cx;
  const dy = ty - cy;
  if (Math.abs(dx) < 1e-6 && Math.abs(dy) < 1e-6) return [cx, cy];
  const sx = Math.abs(dx) > 1e-6 ? halfW / Math.abs(dx) : Infinity;
  const sy = Math.abs(dy) > 1e-6 ? halfH / Math.abs(dy) : Infinity;
  const t = Math.min(sx, sy);
  return [cx + dx * t, cy + dy * t];
}

function num(v, fallback = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function round1(n) {
  return Math.round(n * 10) / 10;
}

function clip(text, n) {
  const s = String(text == null ? "" : text).replace(/\s+/g, " ").trim();
  return s.length > n ? s.slice(0, n - 1) + "…" : s;
}

/** The drawn graph and its side panel, from one row's `graph` object. Returns a `.dlg` div. `row` is the grid row;
 * its processes[] preview supplies a statement the graph left off because it was the same text. */
export function drawDeadlockGraph(graph, row) {
  const nodeW = num(graph.node_width, 240);
  const nodeH = num(graph.node_height, 150);
  const width = Math.max(num(graph.width, 400), nodeW);
  const height = Math.max(num(graph.height, 300), nodeH);
  const processes = Array.isArray(graph.processes) ? graph.processes : [];
  const edges = Array.isArray(graph.edges) ? graph.edges : [];
  const cycles = realCycles(graph);
  const byId = new Map(processes.map((p) => [p.id, p]));
  const previews = new Map();
  for (const q of row && Array.isArray(row.processes) ? row.processes : []) if (q && q.process_id != null) previews.set(q.process_id, q.sql_text);
  for (const p of processes) if (!p.sql_text && previews.get(p.id)) p.sql_text = previews.get(p.id);

  /* A loop's label sits above the card; a card in the top row would clip it, so leave room above the canvas. */
  const topPad = edges.some((e) => e.self) ? 20 : 0;
  const markerId = "dlg-arrow-" + ++markerSeq;
  const canvas = svg("svg", {
    class: "dlg-svg",
    viewBox: "0 " + -topPad + " " + width + " " + (height + topPad),
    width: width,
    height: height + topPad,
    role: "img",
    "aria-label": "Deadlock graph: " + processes.length + " processes",
  });
  const defs = svg("defs");
  const marker = svg("marker", {
    id: markerId,
    viewBox: "0 0 10 10",
    refX: 9,
    refY: 5,
    markerWidth: 8,
    markerHeight: 8,
    orient: "auto-start-reverse",
  });
  marker.appendChild(svg("path", { d: "M 0 0 L 10 5 L 0 10 z", class: "dlg-arrowhead" }));
  defs.appendChild(marker);
  canvas.appendChild(defs);

  /* One dashed frame per independent cycle, only when there is more than one to tell apart. */
  if (cycles.length > 1) {
    let n = 0;
    for (const c of cycles) {
      n++;
      /* Past the cards, so the frame never lies on a card border and its label never covers one. */
      canvas.appendChild(
        svg("rect", {
          class: "dlg-cycle",
          x: num(c.x) - CYCLE_PAD,
          y: num(c.y) - CYCLE_PAD,
          width: num(c.width) + CYCLE_PAD * 2,
          height: num(c.height) + CYCLE_PAD * 2,
          rx: 8,
        })
      );
      canvas.appendChild(svgText({ class: "dlg-cycle-label", x: num(c.x) - CYCLE_PAD + 6, y: num(c.y) - CYCLE_PAD + 14 }, "Cycle " + n));
    }
  }

  const halfW = nodeW / 2;
  const halfH = nodeH / 2;
  const edgeLabels = [];
  for (const e of edges) {
    const waiter = byId.get(e.waiter);
    const owner = byId.get(e.owner);
    if (!waiter || !owner) continue;
    const wx = num(waiter.x) + halfW;
    const wy = num(waiter.y) + halfH;
    const ox = num(owner.x) + halfW;
    const oy = num(owner.y) + halfH;
    let d;
    let ax;
    let ay;
    if (e.self || waiter === owner) {
      /* A parallel thread waiting on its own exchange: a loop over the top of the card. */
      const top = num(waiter.y);
      const cy = top - LOOP_RY;
      canvas.appendChild(svg("ellipse", { class: "dlg-edge dlg-loop", cx: round1(wx), cy: round1(cy), rx: LOOP_RX, ry: LOOP_RY }));
      /* The arrowhead at the foot of the loop, pointing down into the card. */
      canvas.appendChild(svg("path", { class: "dlg-arrowhead dlg-loop-head", d: "M " + round1(wx + 6) + " " + round1(top) + " L " + round1(wx) + " " + round1(top - 12) + " L " + round1(wx + 12) + " " + round1(top - 12) + " z" }));
      d = null;
      ax = wx;
      ay = top - 36; /* above the loop, as the desktop puts it */
    } else {
      const [sx, sy] = clipToRect(wx, wy, ox, oy, halfW, halfH);
      const [ex, ey] = clipToRect(ox, oy, wx, wy, halfW, halfH);
      let dx = ox - wx;
      let dy = oy - wy;
      const len = Math.hypot(dx, dy) || 1;
      dx /= len;
      dy /= len;
      const mx = (sx + ex) / 2 + -dy * BOW;
      const my = (sy + ey) / 2 + dx * BOW;
      d = "M " + round1(sx) + " " + round1(sy) + " Q " + round1(mx) + " " + round1(my) + " " + round1(ex) + " " + round1(ey);
      ax = 0.25 * sx + 0.5 * mx + 0.25 * ex;
      ay = 0.25 * sy + 0.5 * my + 0.25 * ey;
    }
    if (d) canvas.appendChild(svg("path", { class: "dlg-edge", d, "marker-end": "url(#" + markerId + ")" }));
    const label = [shortResource(e.resource_label) || e.resource_kind || "", e.request_mode || ""].filter(Boolean).join(" · ");
    if (label) {
      const t = svgText({ class: "dlg-edge-label", x: round1(ax), y: round1(ay), "text-anchor": "middle" }, label);
      const tip = svg("title");
      tip.textContent =
        (e.resource_label ? e.resource_label + "\n" : "") +
        "Waiter requests " + (e.request_mode || "?") + ", owner holds " + (e.owner_mode || "?") + (e.resource_kind ? " (" + e.resource_kind + ")" : "");
      t.appendChild(tip);
      edgeLabels.push(t);
    }
  }
  /* Labels last, so a card or a curve never hides one. */
  for (const t of edgeLabels) canvas.appendChild(t);

  const side = el("div", { class: "dlg-side" });
  const cards = new Map();

  const select = (p) => {
    for (const [, g] of cards) g.setAttribute("class", g.getAttribute("class").replace(" selected", ""));
    const g = cards.get(p.id);
    if (g) g.setAttribute("class", g.getAttribute("class") + " selected");
    side.textContent = "";
    side.appendChild(el("div", { class: "dlg-side-head", text: "SPID " + p.spid + (p.victim ? " (victim)" : "") }));
    const fields = [
      ["Process", p.id],
      ["ECID", p.ecid ? String(p.ecid) : null],
      ["Procedure", p.proc_name],
      ["Contended object", p.contended_object],
      ["Lock mode", p.lock_mode],
      ["Wait resource", p.wait_resource],
      ["Wait time (ms)", p.wait_time_ms != null ? String(p.wait_time_ms) : null],
      ["Priority", p.priority != null ? String(p.priority) : null],
      ["Database", p.database_name],
      ["Login", p.login_name],
      ["Host", p.host_name],
      ["App", p.client_app],
      ["Isolation", p.isolation_level],
      ["Status", p.status],
    ];
    for (const [k, v] of fields) {
      if (v == null || v === "") continue;
      side.appendChild(el("div", { class: "dlg-prop" }, [el("span", { class: "fk", text: k + ": " }), el("span", { class: "fv", text: v })]));
    }
    if (p.sql_text) {
      side.appendChild(el("pre", { class: "code", text: p.sql_text }));
      if (p.sql_text_cut) side.appendChild(el("div", { class: "dlg-prop", text: "The statement is cut here; the whole text is in the XML." }));
    } else {
      side.appendChild(el("div", { class: "dlg-prop", text: "No statement text was captured for this process." }));
    }
  };

  for (const p of processes) {
    const g = svg("g", {
      class: "dlg-node" + (p.victim ? " victim" : ""),
      tabindex: 0,
      role: "button",
      transform: "translate(" + num(p.x) + " " + num(p.y) + ")",
    });
    g.appendChild(svg("rect", { class: "dlg-card", width: nodeW, height: nodeH, rx: 6 }));
    g.appendChild(svgText({ class: "dlg-spid", x: 10, y: 22 }, "SPID " + p.spid + (p.ecid ? " / " + p.ecid : "")));
    if (p.victim) g.appendChild(svgText({ class: "dlg-badge", x: nodeW - 10, y: 22, "text-anchor": "end" }, "VICTIM"));
    const lines = [
      p.proc_name || "",
      p.contended_object || "",
      [p.lock_mode ? "lock " + p.lock_mode : "", p.wait_time_ms != null ? "wait " + p.wait_time_ms + " ms" : ""].filter(Boolean).join(" · "),
      clip(p.sql_text, SQL_PREVIEW),
    ];
    let y = 46;
    for (const line of lines) {
      if (line) g.appendChild(svgText({ class: "dlg-line", x: 10, y }, clip(line, 34)));
      y += 24;
    }
    const pick = () => select(p);
    g.addEventListener("click", pick);
    g.addEventListener("keydown", (ev) => {
      if (ev && (ev.key === "Enter" || ev.key === " ")) {
        if (ev.preventDefault) ev.preventDefault();
        pick();
      }
    });
    cards.set(p.id, g);
    canvas.appendChild(g);
  }

  const first = processes.find((p) => p.victim) || processes[0];
  if (first) select(first);

  const scroll = el("div", { class: "dlg-scroll" }, [canvas]);
  return el("div", { class: "dlg" }, [scroll, side]);
}
