/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * Fleet Sweeps page (#3466, lane 3) — the sweep-with-memory timeline from GET /api/sweeps. The API is
 * PRE-JUDGED: every band, transition, liveness verdict and would-have-paged row was computed and PERSISTED by
 * the sweep engine, and this page only renders the documents (R1 — no threshold is re-derived here). Three
 * honesty rules the render carries, because they are the feature's own contracts:
 *   - The MUTE HEADER shows on every sweep taken under alerts_enabled: false, and the would-have-paged ledger
 *     renders whenever the document carries one — INCLUDING empty ("muted, and nothing would have paged" is a
 *     statement the operator is owed). The mute silences the alert engine, never this page.
 *   - QUIET IS NOT CLEAN: a sweep with instruments_alive: false looks DIFFERENT — a red banner on the
 *     document, a red edge + badge on its timeline row — never a green sweep with a footnote.
 *   - The diff is the point: verdicts render previous band beside band, and a first sweep says
 *     "no previous sweep" rather than implying a quiet history.
 * The viewing span is user-configurable, DEFAULT HOURLY (the owner's ruling); the sweep CADENCE is read from
 * get_alert_settings' fleet_sweep group for display only — the Settings window and the alert-settings write
 * tool own that knob, and this page has no write affordance at all.
 */

import { el, mount, apiGet, readTool, loadingStrip, errorStrip, emptyStrip, localTime, relTime, rollupTextId,
         fmtInt, bandClass, disclosure } from "../util.js";

/* The watch-item state vocabulary — the label each FleetSweepWatchStateMachine state owes an operator. The
   KEYS are the machine's own constants; Darling.Tests.FleetSweepWebFeedTests parses THIS OBJECT and compares
   it key-by-key against those constants (the ALERT_STATE_LABELS parity idiom), so the store, the wire and
   this render cannot drift into three vocabularies. */
export const SWEEP_WATCH_STATE_LABELS = {
  pending: "Pending",
  open: "Open",
  carried: "Carried",
  closed: "Closed",
};

/* The configurable viewing spans. 1 hour is the DEFAULT by the owner's ruling ("configurable spans of time
   though, default hourly") — at the shipped hourly cadence that lands the page on the newest sweep, and the
   wider spans are the way into history. */
const SPANS = [
  { hours: 1, label: "Last hour" },
  { hours: 6, label: "6 hours" },
  { hours: 24, label: "24 hours" },
  { hours: 72, label: "3 days" },
  { hours: 168, label: "7 days" },
];

/* Page state persists across the 60s refresh (the fleet page's module-level rule): the chosen span, the
   selected sweep (null = latest), and whether the watch table is showing closed history. */
let sweepSpanHours = 1;
let selectedSweepId = null;
let watchStateShown = null; // null = the open + carried default view; "closed" etc. on request

export async function renderSweeps(main) {
  const detailBox = el("div", {});
  const timelineBox = el("div", {});
  const watchBox = el("div", {});
  const cadenceMeta = el("div", { class: "meta", text: "" });

  mount(main, [
    el("div", { class: "page-head" }, [
      el("h2", { text: "Fleet Sweeps" }),
      el("div", { class: "spacer" }),
      spanControl(),
      cadenceMeta,
    ]),
    detailBox,
    el("h3", { class: "section-title", text: "Timeline" }),
    timelineBox,
    el("h3", { class: "section-title", text: "Watch items" }),
    watchBox,
  ]);

  /* Independent sections load in parallel; each degrades alone (the triage page's rule). */
  renderCadence(cadenceMeta);
  renderDetail(detailBox);
  renderTimeline(timelineBox, detailBox);
  renderWatchItems(watchBox);
}

/* ─────────────────────────── the cadence display (read-only) ─────────────────────────── */

/* The effective cadence, read from the same get_alert_settings the Settings window edits. Display only:
   the web feed reads the knob, it never writes it (lane 2's V124 owns the writes). */
async function renderCadence(meta) {
  const res = await readTool("get_alert_settings", {});
  const fs = res.kind === "data" && res.data ? res.data.fleet_sweep : null;
  if (!fs) return;
  meta.textContent = fs.enabled
    ? "sweeps every " + fmtInt(fs.interval_minutes) + " min"
    : "sweeps disabled (fleet_sweep.enabled: false)";
}

function spanControl() {
  const sel = el("select", { class: "sort-select", "aria-label": "Timeline span" },
    SPANS.map((s) => el("option", { value: String(s.hours), text: s.label })));
  sel.value = String(sweepSpanHours);
  sel.addEventListener("change", () => {
    sweepSpanHours = Number(sel.value);
    const mainNode = document.getElementById("main");
    if (mainNode) renderSweeps(mainNode);
  });
  return el("label", { class: "sort-control" }, [el("span", { text: "Span" }), sel]);
}

/* ─────────────────────────── the sweep document ─────────────────────────── */

async function renderDetail(box) {
  mount(box, loadingStrip("Loading sweep…"));
  /* sweep_id arrives as a JSON STRING (#3487): the ids are tick-scale, ~70x past
     Number.MAX_SAFE_INTEGER, so a JSON number here had already been rounded by JSON.parse and this
     fetch asked for a neighbor that was never recorded. The string rides this page's existing code
     unchanged: the concat below concatenates it verbatim, the timeline's active check compares it
     strictly against a value only ever assigned from s.sweep_id (like with like), and the
     no-previous-sweep test is previous_sweep_id == null, which a non-empty string never satisfies.
     Nothing on this page does arithmetic on an id, and the timeline's order is the API's own
     (by swept_at) — keep it that way: an id is a name here, not a number. */
  const res = await apiGet(selectedSweepId != null ? "/api/sweeps/" + selectedSweepId : "/api/sweeps/latest");
  if (res.kind === "error") {
    if (res.status === 404) {
      /* Absence is not a fault: no sweep yet (fresh install / engine off), or a selected sweep that
         retention has pruned. The API's own sentence says which. */
      mount(box, emptyStrip(res.message + " Sweeps appear here once fleet_sweep.enabled is on and the cadence elapses."));
      return;
    }
    mount(box, errorStrip(res.message));
    return;
  }

  mount(box, sweepDocument(res.data));
}

/** Bands come out of the daily-summary calculator ("Healthy"/"Warning"/"Critical"/"No Data"); "No Data" is
    not a CSS-safe class name and renders in the neutral Unknown treatment. */
function sweepBandClass(band) {
  return bandClass(band === "No Data" ? "Unknown" : band);
}

function sweepDocument(d) {
  const report = d.report && typeof d.report === "object" ? d.report : {};
  const nodes = [];

  nodes.push(el("div", { class: "sweep-doc-head" }, [
    el("h3", { text: "Sweep at " + localTime(d.swept_at) }),
    el("span", { class: "meta", text: "span " + localTime(d.span_start) + " → " + localTime(d.span_end) }),
    d.previous_sweep_id == null
      ? el("span", { class: "badge band-Offline", text: "no previous sweep" })
      : null,
  ]));

  /* THE MUTE HEADER — the muted-mode contract: the state is stated on every sweep so it cannot fade from
     operator memory, and the ledger below is what silence cost. */
  if (!d.alerts_enabled) {
    nodes.push(el("div", { class: "sweep-banner muted-sweep", role: "status" }, [
      el("span", { class: "glyph", text: "⊘" }),
      el("span", { text: "Alert delivery was OFF for this sweep (master switch). The would-have-paged ledger below records what would have reached the channel." }),
    ]));
  }

  /* QUIET IS NOT CLEAN — a sweep that could not prove its instruments must LOOK different. role=alert:
     this is the render of a loud failure, never a footnote on a green page. */
  if (!d.instruments_alive) {
    nodes.push(el("div", { class: "sweep-banner instruments-dead", role: "alert" }, [
      el("span", { class: "glyph", text: "✕" }),
      el("span", { text: "Instruments dead: this sweep could not prove its data sources were alive, so its quiet is unproven — not clean. The liveness block below names the dead instrument." }),
    ]));
  }

  if (report.post_restart_window) {
    nodes.push(el("div", { class: "strip notice", role: "status" },
      ["This sweep ran inside the post-restart settle window — fleet staleness here is classified startup-transient, not an outage."]));
  }

  /* The header tiles: counts the document already carries. Built through ONE closure with the number
     aria-describedby-tied to its label — the #3031/#3045 wiring the fleet and AG rollups carry, deduped
     per render so the ids stay stable across the poll. */
  const fleet = report.fleet || {};
  const bands = fleet.bands || {};
  const watch = report.watch || {};
  const whp = Array.isArray(d.would_have_paged) ? d.would_have_paged : null;
  const usedIds = new Set();
  const tile = (value, label, extraClass) => {
    const lblId = rollupTextId(label, "lbl", usedIds);
    return el("div", { class: "tile" + (extraClass ? " " + extraClass : "") }, [
      el("div", { class: "num", text: String(value), "aria-describedby": lblId }),
      el("div", { class: "lbl", id: lblId, text: label }),
    ]);
  };
  nodes.push(el("div", { class: "sweep-tiles" }, [
    tile(fmtInt(d.servers_reported) + " / " + fmtInt(d.servers_expected), "servers reported"),
    ...Object.keys(bands).sort().map((b) =>
      tile(fmtInt(bands[b]), b, "sweep-tile-" + sweepBandClass(b))),
    tile(fmtInt(count(watch.opened)) + " / " + fmtInt(count(watch.closed)), "watch opened / closed"),
    whp ? tile(fmtInt(whp.length), "would have paged", whp.length ? "sweep-tile-band-Critical" : null) : null,
  ]));

  /* Changes since the previous sweep — the diff is the feature's acceptance test, so it leads. */
  const changes = report.changes || {};
  const transitions = asArray(changes.band_transitions);
  nodes.push(el("h4", { class: "sweep-section", text: "Changes since previous sweep" }));
  if (report.no_previous_sweep) {
    nodes.push(emptyStrip("First sweep — there is no previous sweep to diff against, so this document states absolutes."));
  } else if (!transitions.length && !count(changes.new_servers) && !count(changes.departed_servers)) {
    nodes.push(emptyStrip("No band changes, arrivals or departures since the previous sweep."));
  } else {
    if (transitions.length) {
      nodes.push(table(
        ["Server", "From", "To", "Reason"],
        transitions.map((t) => [
          cellText(t.server),
          bandCell(t.from),
          bandCell(t.to),
          cellText(t.reason || "—", "wrap"),
        ])));
    }
    for (const [label, list] of [["New servers", changes.new_servers], ["Departed servers", changes.departed_servers]]) {
      const names = asArray(list);
      if (names.length) {
        nodes.push(el("div", { class: "sweep-line", text: label + ": " + names.join(", ") }));
      }
    }
  }

  /* The would-have-paged ledger — rendered whenever the document carries one (every master-off sweep),
     empty included: the operator sees what silence cost, or that it cost nothing. */
  if (whp) {
    nodes.push(el("h4", { class: "sweep-section", text: "Would have paged (master switch off)" }));
    nodes.push(whp.length
      ? table(
          ["Server", "Family", "Evidence"],
          whp.map((w) => [
            cellText(w.server || "server " + w.server_id),
            cellText(w.family),
            evidenceCell(w.evidence),
          ]))
      : emptyStrip("Muted, and nothing crossed a critical trigger in this span — the mute cost nothing this sweep."));
  }

  /* Per-server verdicts: previous band beside band, reason for every non-Healthy card. */
  const verdicts = Array.isArray(d.verdicts) ? d.verdicts : [];
  nodes.push(el("h4", { class: "sweep-section", text: "Per-server verdicts" }));
  nodes.push(verdicts.length
    ? table(
        ["Server", "Previous", "Band", "Reason"],
        verdicts.map((v) => [
          cellText(v.server),
          bandCell(v.previous_band || "—"),
          bandCell(v.band, v.band_changed),
          cellText(v.reason || "—", "wrap"),
        ]))
    : emptyStrip("This sweep carries no per-server verdicts."));

  /* The instrument-liveness block, rendered honestly: the verdict, its counters, and every note the
     engine wrote about what it could and could not judge. */
  const liveness = d.instrument_liveness && typeof d.instrument_liveness === "object" ? d.instrument_liveness : {};
  nodes.push(el("h4", { class: "sweep-section", text: "Instrument liveness" }));
  nodes.push(livenessBlock(d, liveness));

  return el("div", { class: "sweep-doc card" + (d.instruments_alive ? "" : " instruments-dead") }, nodes);
}

function livenessBlock(d, lv) {
  const rows = [];
  rows.push(["Verdict", d.instruments_alive ? "alive" : "DEAD", d.instruments_alive ? "sev-Healthy" : "sev-Critical"]);
  rows.push(["Service started", localTime(lv.service_started_at)]);
  if (lv.baseline_reset) rows.push(["Pass-counter baseline", "reset by restart — advance not judgeable this sweep"]);
  rows.push(["Alert passes", fmtInt(lv.alert_passes_total)
    + (lv.alert_passes_previous != null ? " (previous " + fmtInt(lv.alert_passes_previous) + ")" : " (no baseline)")
    + (lv.alert_passes_advancing === true ? " — advancing" : lv.alert_passes_advancing === false ? " — frozen" : "")]);
  rows.push(["Alert read failures", fmtInt(lv.alert_read_failures_total)]);

  const faults = asArray(lv.server_read_faults);
  const notes = asArray(lv.notes);
  return el("div", { class: "liveness-block" }, [
    table(["Instrument", "Reading"], rows.map((r) => [
      cellText(r[0]),
      el("td", { class: r[2] || null, text: r[1] }),
    ])),
    faults.length
      ? el("div", { class: "sweep-line sev-Critical", text: faults.length + " server read fault(s): "
          + faults.map((f) => f.server + " (" + f.error + ")").join("; ") })
      : null,
    notes.length
      ? el("ul", { class: "liveness-notes" }, notes.map((n) => el("li", { text: String(n) })))
      : null,
  ]);
}

/* ─────────────────────────── the timeline ─────────────────────────── */

async function renderTimeline(box, detailBox) {
  mount(box, loadingStrip("Loading timeline…"));
  const res = await apiGet("/api/sweeps?hours=" + sweepSpanHours);
  if (res.kind === "error") return mount(box, errorStrip(res.message));

  const sweeps = (res.data && res.data.sweeps) || [];
  if (!sweeps.length) {
    mount(box, emptyStrip("No sweeps in this span. Widen the span, or check that fleet_sweep.enabled is on — a gap in this timeline is a missed sweep, with its cause on the service log."));
    return;
  }

  mount(box, el("div", { class: "sweep-timeline" }, sweeps.map((s) => timelineRow(s, box, detailBox))));
}

function timelineRow(s, box, detailBox) {
  const report = s.report && typeof s.report === "object" ? s.report : {};
  const bands = (report.fleet && report.fleet.bands) || {};
  const watch = report.watch || {};
  const whpCount = count(report.would_have_paged);
  const active = selectedSweepId === s.sweep_id;

  /* A sweep that could not prove its instruments LOOKS different in the list too — a red edge and a
     badge, so the timeline cannot read as an unbroken row of green documents. */
  const cls = "sweep-row" + (s.instruments_alive ? "" : " instruments-dead") + (active ? " active" : "");

  let row;
  const select = () => {
    selectedSweepId = s.sweep_id;
    box.querySelectorAll(".sweep-row").forEach((r) => r.classList.remove("active"));
    row.classList.add("active");
    renderDetail(detailBox);
  };
  row = el("div", {
    class: cls,
    onActivate: select,
  }, [
    el("span", { class: "sweep-time", text: localTime(s.swept_at), title: relTime(s.swept_at) }),
    s.instruments_alive ? null : el("span", { class: "badge band-Critical", text: "INSTRUMENTS DEAD" }),
    s.alerts_enabled ? null : el("span", { class: "badge band-Warning", text: "MUTED" }),
    el("span", { class: "sweep-cell", text: fmtInt(s.servers_reported) + "/" + fmtInt(s.servers_expected) + " reported" }),
    el("span", { class: "sweep-cell" },
      Object.keys(bands).sort().map((b) =>
        el("span", { class: "sweep-band-chip " + sweepBandClass(b), text: bands[b] + " " + b }))),
    count(watch.opened) ? el("span", { class: "sweep-cell sev-Warning", text: "+" + count(watch.opened) + " watch" }) : null,
    count(watch.closed) ? el("span", { class: "sweep-cell sev-Healthy", text: "−" + count(watch.closed) + " watch" }) : null,
    whpCount ? el("span", { class: "sweep-cell sev-Critical", text: whpCount + " would have paged" }) : null,
  ]);
  return row;
}

/* ─────────────────────────── the watch-item worklist ─────────────────────────── */

async function renderWatchItems(box) {
  mount(box, loadingStrip("Loading watch items…"));
  const query = watchStateShown ? "?state=" + encodeURIComponent(watchStateShown) : "";
  const res = await apiGet("/api/sweeps/watch-items" + query);
  if (res.kind === "error") return mount(box, errorStrip(res.message));

  const d = res.data || {};
  const items = d.items || [];
  const toggle = el("label", { class: "attention-control", title: "Show closed watch items (the episode history) instead of the open + carried worklist." }, [
    (() => {
      const cb = el("input", { type: "checkbox", "aria-label": "Show closed watch items" });
      cb.checked = watchStateShown === "closed";
      cb.addEventListener("change", () => {
        watchStateShown = cb.checked ? "closed" : null;
        renderWatchItems(box);
      });
      return cb;
    })(),
    el("span", { text: "Show closed" }),
  ]);

  const label = watchStateShown
    ? (SWEEP_WATCH_STATE_LABELS[watchStateShown] || watchStateShown) + " watch items"
    : "Open right now (open + carried)";

  const body = items.length
    ? table(
        ["Server", "Item", "State", "Position", "First seen", "Last seen", "Condition / evidence"],
        items.map((w) => [
          /* The display name the feed joins from the retained verdict history (#3482); the bare id is
             the degrade for a name the store no longer holds, and the fleet-scope sentinel renders as
             the fleet — it has no server name, and "server 0" would be a fabrication. */
          cellText(w.fleet_scope ? "Fleet" : (w.server || "server " + w.server_id)),
          cellText(w.item, "mono"),
          el("td", { class: watchStateSev(w.state), text: SWEEP_WATCH_STATE_LABELS[w.state] || w.state }),
          cellText(watchPosition(w, d)),
          cellText(relTime(w.first_seen_at)),
          cellText(relTime(w.last_seen_at)),
          el("td", { class: "wrap" }, [
            w.evidence != null
              ? disclosure(w.condition, el("pre", { class: "sweep-evidence", text: pretty(w.evidence) }))
              : el("span", { text: w.condition }),
          ]),
        ]))
    : emptyStrip(watchStateShown
        ? "No " + (SWEEP_WATCH_STATE_LABELS[watchStateShown] || watchStateShown).toLowerCase() + " watch items."
        : "Nothing is open right now — no watch item is open or carried.");

  mount(box, [
    el("div", { class: "sweep-watch-head" }, [el("span", { class: "meta", text: label }), el("div", { class: "spacer" }), toggle]),
    body,
  ]);
}

/** The hysteresis position, rendered against the bars the API carries — never a hardcoded 2. */
function watchPosition(w, d) {
  if (w.state === "pending") return w.consecutive_hits + " of " + d.entry_bar_sweeps + " hits to open";
  if (w.state === "closed") return "closed";
  if (w.consecutive_misses > 0) return w.consecutive_misses + " of " + d.exit_bar_sweeps + " quiet to close";
  return w.consecutive_hits + " consecutive hit" + (w.consecutive_hits === 1 ? "" : "s");
}

function watchStateSev(state) {
  if (state === "open") return "sev-Critical";
  if (state === "carried") return "sev-Warning";
  if (state === "pending") return "sev-Unknown";
  return "sev-Healthy";
}

/* ─────────────────────────── small shared builders ─────────────────────────── */

function table(headers, rows) {
  return el("div", { class: "table-wrap" }, [
    el("table", { class: "data" }, [
      el("thead", {}, [el("tr", {}, headers.map((h) => el("th", { text: h })))]),
      el("tbody", {}, rows.map((cells) => el("tr", {}, cells))),
    ]),
  ]);
}

function cellText(text, cls) {
  return el("td", { class: cls || null, text: text == null ? "—" : String(text) });
}

function bandCell(band, changed) {
  return el("td", {}, [
    el("span", { class: "badge " + sweepBandClass(band), text: band }),
    changed ? el("span", { class: "sweep-changed", text: " changed" }) : null,
  ]);
}

function evidenceCell(evidence) {
  return el("td", { class: "wrap" }, [
    disclosure(pretty(evidence), el("pre", { class: "sweep-evidence", text: pretty(evidence) })),
  ]);
}

function pretty(v) {
  if (v == null) return "—";
  if (typeof v === "string") return v;
  try { return JSON.stringify(v, null, 1); } catch { return String(v); }
}

function asArray(v) {
  return Array.isArray(v) ? v : [];
}

function count(v) {
  return Array.isArray(v) ? v.length : 0;
}
