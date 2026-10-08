/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The web time range picker (#5562): a button naming the range, the resolved start, end and zone beside it, and a popup with the
 * short presets, the calendar periods (each with its current length), a text box that shows the parsed range BEFORE Apply (or
 * why it was refused), and a date and time pick. The grammar and every rule live in time-range.js (the twin of the desktop
 * model, pinned by the shared fixture); this file is only the DOM.
 *
 *   const picker = timeRangePicker({ spec, zone, reachHours, onChange: (spec, range) => redraw() });
 *   host.appendChild(picker.node);
 *   picker.window();      // { hours, asOf, live, startMs, endMs } for the reads, or null when the range cannot be resolved now
 *   picker.resolve();     // the resolved range at the current clock (a live range slides), or null
 *   picker.setSpec(spec); // hold a range without raising onChange
 *
 * Keyboard: Enter in the text box applies, Esc closes the popup and returns focus to the button. Presets and periods are real
 * buttons. A period longer than the reach (hours) or shorter than 5 minutes is disabled and says why. Nothing here touches the
 * document until the popup opens, and the popup's own listeners are removed when it closes.
 */

import { el } from "./util.js";
import {
  ROLLING_PRESETS,
  CALENDAR_PRESETS,
  browserZone,
  specId,
  specName,
  specFromId,
  relativeSpec,
  resolveSpec,
  parseRange,
  readWindow,
  reachRefusal,
  reachText,
  sampleIntervalNote,
  dataStartNote,
  wallText,
} from "./time-range.js";

export const DEFAULT_REACH_HOURS = 168;

const DAY_MS = 86400000;
let pickerCount = 0;

/**
 * @param {object} [opts]
 * @param {object} [opts.spec] the range to start on (default the past day)
 * @param {string} [opts.zone] the IANA zone times are typed and shown in (default the browser's)
 * @param {() => number} [opts.now] the clock, epoch ms (default Date.now); tests give their own
 * @param {number} [opts.reachHours] the longest range the page's reads take, in hours (default 168); longer ones are greyed out
 * @param {number|null} [opts.sampleIntervalMs] the main collector's interval; a span with fewer than 3 samples shows a note
 * @param {number|null} [opts.dataStartMs] where the data begins; a range starting well before it shows a note
 * @param {boolean} [opts.compact] hide the detail beside the button (it moves into the tooltip)
 * @param {string} [opts.label] the accessible name (default "Time range")
 * @param {(spec: object, range: object) => void} [opts.onChange] raised when the reader picks or applies a range
 */
export function timeRangePicker(opts = {}) {
  const zone = opts.zone || browserZone();
  const now = opts.now || (() => Date.now());
  const name = opts.label || "Time range";
  const previewId = "trp-preview-" + ++pickerCount;
  let spec = opts.spec || relativeSpec(DAY_MS);
  let reach = opts.reachHours > 0 ? opts.reachHours : DEFAULT_REACH_HOURS;
  let sampleIntervalMs = opts.sampleIntervalMs || null;
  let dataStartMs = opts.dataStartMs ?? null;
  const onChange = opts.onChange || (() => {});
  let open = false;
  let popup = null;
  let textBox = null;
  let preview = null;
  let applyButton = null;
  let pending = null;
  let fromBox = null;
  let toBox = null;
  let removeDocumentListeners = null;

  const button = el("button", { type: "button", class: "trp-button", "aria-haspopup": "dialog", "aria-expanded": "false" });
  const detail = el("span", { class: "trp-detail" });
  const note = el("span", { class: "trp-note" });
  const popupSlot = el("div", { class: "trp-popup-slot" });
  const node = el("div", { class: "time-range-picker" + (opts.compact ? " trp-compact" : "") }, [button, detail, note, popupSlot]);

  /* The range as it resolves right now, or the error that stops it (a calendar period under 5 minutes old, say). */
  function current() {
    return resolveSpec(spec, now(), zone);
  }

  function draw() {
    const r = current();
    if (!r.ok) {
      button.textContent = specName(spec);
      button.setAttribute("aria-label", name + ": " + specName(spec) + ". " + r.error.message);
      detail.textContent = r.error.message;
      node.setAttribute("title", r.error.message);
      note.textContent = "";
      return;
    }
    const range = r.range;
    const text = spec.kind === "fixed" || spec.kind === "since" ? "Custom (" + range.length + ")" : specName(spec);
    const where = range.startText + " - " + range.endText + " (" + range.zoneText + ")";
    button.textContent = text;
    button.setAttribute("aria-label", name + ": " + text + ", " + where);
    detail.textContent = opts.compact ? "" : where;
    node.setAttribute("title", opts.compact ? text + ", " + where : "");
    const notes = [sampleIntervalNote(range.spanMs, sampleIntervalMs), dataStartNote(range, dataStartMs)].filter(Boolean);
    note.textContent = notes.join(" ");
  }

  function hold(next) {
    spec = next;
    draw();
  }

  function choose(next) {
    const r = resolveSpec(next, now(), zone);
    if (!r.ok) return r.error.message;
    const tooLong = reachRefusal(r.range.spanMs, reach);
    if (tooLong) return tooLong;
    hold(next);
    close(true);
    onChange(next, r.range);
    return null;
  }

  /* One row of the popup lists: the name, the length it is right now, and why it is greyed out when it is. */
  function item(next, nowMs) {
    const r = resolveSpec(next, nowMs, zone);
    const why = !r.ok ? r.error.message : reachRefusal(r.range.spanMs, reach);
    const selected = specId(next) === specId(spec);
    const children = [el("span", { class: "trp-item-name", text: specName(next) })];
    if (r.ok) children.push(el("span", { class: "trp-item-length", text: r.range.length }));
    if (why) children.push(el("span", { class: "trp-item-why", text: why }));
    const props = { type: "button", class: "trp-item" + (selected ? " trp-selected" : "") + (why ? " trp-disabled" : ""), "aria-pressed": selected ? "true" : "false" };
    if (why) {
      props["aria-disabled"] = "true";
      props.title = why;
    }
    const b = el("button", props, children);
    b.addEventListener("click", () => {
      if (why) return;
      choose(next);
    });
    return b;
  }

  /* Shows what the text box means, or why it does not mean anything, before Apply. */
  function showPreview() {
    pending = null;
    const text = textBox.value;
    if (text.trim() === "") {
      preview.textContent = "Type a range, or pick one on the left.";
      preview.className = "trp-preview";
      applyButton.disabled = true;
      return;
    }
    const nowMs = now();
    const r = parseRange(text, nowMs, zone);
    let error = r.ok ? null : r.error;
    if (r.ok) {
      const tooLong = reachRefusal(r.range.spanMs, reach);
      if (tooLong) error = tooLong;
    }
    if (error) {
      preview.textContent = error;
      preview.className = "trp-preview trp-error";
      applyButton.disabled = true;
      return;
    }
    pending = r;
    const notes = [sampleIntervalNote(r.range.spanMs, sampleIntervalMs), dataStartNote(r.range, dataStartMs)].filter(Boolean);
    preview.textContent = [r.echo, ...notes].join(" ");
    preview.className = "trp-preview trp-ok";
    applyButton.disabled = false;
  }

  function apply() {
    if (!pending) {
      showPreview();
      if (!pending) return;
    }
    choose(pending.spec);
  }

  /* The date and time boxes write the typed form (in the picker's zone), so one path parses, previews and applies. */
  function pickChanged() {
    if (!fromBox.value || !toBox.value) return;
    textBox.value = fromBox.value.replace("T", " ") + " - " + toBox.value.replace("T", " ");
    showPreview();
  }

  function buildPopup() {
    const nowMs = now();
    const r = current();
    textBox = el("input", { type: "text", class: "trp-text", "aria-label": "Type a time range", "aria-describedby": previewId, placeholder: "45m, last month, Oct 1 - Oct 2", autocomplete: "off", spellcheck: "false" });
    preview = el("div", { class: "trp-preview", id: previewId, role: "status", "aria-live": "polite" });
    applyButton = el("button", { type: "button", class: "btn trp-apply", text: "Apply" });
    fromBox = el("input", { type: "datetime-local", class: "trp-pick", "aria-label": "Range start", step: "60" });
    toBox = el("input", { type: "datetime-local", class: "trp-pick", "aria-label": "Range end", step: "60" });
    if (r.ok) {
      fromBox.value = wallText(r.range.startMs, zone).replace(" ", "T").slice(0, 16);
      toBox.value = wallText(r.range.endMs, zone).replace(" ", "T").slice(0, 16);
    }
    textBox.addEventListener("input", showPreview);
    textBox.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        apply();
      }
    });
    applyButton.addEventListener("click", apply);
    fromBox.addEventListener("change", pickChanged);
    toBox.addEventListener("change", pickChanged);

    const quick = el("div", { class: "trp-column" }, [
      el("div", { class: "trp-heading", text: "Quick ranges" }),
      ...ROLLING_PRESETS.map((s) => item(s, nowMs)),
    ]);
    const periods = el("div", { class: "trp-column" }, [
      el("div", { class: "trp-heading", text: "Calendar periods" }),
      ...CALENDAR_PRESETS.map((s) => item(s, nowMs)),
    ]);
    const custom = el("div", { class: "trp-column trp-custom" }, [
      el("div", { class: "trp-heading", text: "Type a range" }),
      textBox,
      preview,
      el("div", { class: "trp-heading", text: "Or pick a start and an end" }),
      el("div", { class: "trp-picks" }, [fromBox, el("span", { text: "to" }), toBox]),
      el("div", { class: "trp-zone", text: "Times are in " + zone.replace(/_/g, " ") + "." }),
      applyButton,
    ]);
    popup = el("div", { class: "trp-popup", role: "dialog", "aria-label": name + " options" }, [quick, periods, custom]);
    popup.addEventListener("keydown", (e) => {
      if (e.key === "Escape") {
        e.stopPropagation();
        close(true);
      }
    });
    showPreview();
  }

  function openPopup() {
    if (open) return;
    open = true;
    buildPopup();
    popupSlot.appendChild(popup);
    button.setAttribute("aria-expanded", "true");
    /* A click anywhere else closes the popup. The listener exists only while the popup is open. */
    const outside = (e) => {
      if (!node.contains || !node.contains(e.target)) close(false);
    };
    document.addEventListener("mousedown", outside);
    removeDocumentListeners = () => document.removeEventListener("mousedown", outside);
    if (textBox.focus) textBox.focus();
  }

  function close(refocus) {
    if (!open) return;
    open = false;
    if (removeDocumentListeners) removeDocumentListeners();
    removeDocumentListeners = null;
    if (popup) popupSlot.removeChild(popup);
    popup = null;
    button.setAttribute("aria-expanded", "false");
    if (refocus && button.focus) button.focus();
  }

  button.addEventListener("click", () => (open ? close(true) : openPopup()));
  button.addEventListener("keydown", (e) => {
    if (e.key === "Escape") close(true);
  });
  draw();

  return {
    node,
    /** The range as it resolves now, or null when it cannot be (a live range slides with the clock). */
    resolve() {
      const r = current();
      return r.ok ? r.range : null;
    },
    /** The window the reads take for the range now ({ hours, asOf, live, startMs, endMs }), or null. */
    window() {
      const r = current();
      return r.ok ? readWindow(r.range) : null;
    },
    spec() {
      return spec;
    },
    /** Hold a range without raising onChange (a stored choice, a deep link). A spec the grammar can read back by id works too. */
    setSpec(next) {
      const s = typeof next === "string" ? specFromId(next) : next;
      if (s) hold(s);
    },
    /** Pick a range as if the reader had: holds it, closes the popup and raises onChange. Returns the refusal, or null. */
    select(next) {
      return choose(next);
    },
    /** Redraw the label and detail (a live range slides; call on a refresh tick). */
    refresh: draw,
    setReach(hours) {
      reach = hours > 0 ? hours : DEFAULT_REACH_HOURS;
      if (open) {
        close(false);
        openPopup();
      }
    },
    setSampleInterval(ms) {
      sampleIntervalMs = ms || null;
      draw();
    },
    setDataStart(ms) {
      dataStartMs = ms ?? null;
      draw();
    },
    openPopup,
    close: () => close(true),
    isOpen: () => open,
    /** The longest range the page takes, in words ("7 days"). */
    reachText: () => reachText(reach),
  };
}
