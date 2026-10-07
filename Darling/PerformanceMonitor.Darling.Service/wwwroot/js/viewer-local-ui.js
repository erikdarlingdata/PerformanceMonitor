/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The controls for the per-browser conveniences in viewer-local.js (#4843): the favourite star, the alert-count
 * badge with its acknowledge click, the severity-colour settings and the sidebar collapse button. Text only, via
 * el(); a stored colour reaches CSS only through viewer-local's #rrggbb check and a custom property, never markup.
 */

import { el, mount } from "./util.js";
import {
  SEVERITIES, DEFAULT_SEVERITY_COLORS, isFavorite, toggleFavorite, attentionFor, acknowledge, severityColor,
  setSeverityColor, resetSeverityColors, applySeverityColors, isSidebarCollapsed, setSidebarCollapsed, onChange,
} from "./viewer-local.js";

/** Keeps a control inside a clickable row from also activating the row (a click, or Enter/Space on the button). */
function inertToRow(node) {
  node.addEventListener("keydown", (e) => e.stopPropagation());
  return node;
}

/** The favourite star for a server: a real button, pressed when the server is a favourite. */
export function favoriteStar(serverId, onToggled) {
  const on = isFavorite(serverId);
  const btn = el("button", {
    type: "button",
    class: "fav-star" + (on ? " on" : ""),
    "aria-pressed": on ? "true" : "false",
    title: on ? "Remove from favourites" : "Add to favourites",
    "aria-label": on ? "Remove from favourites" : "Add to favourites",
    text: on ? "\u2605" : "\u2606",
    onClick: (e) => {
      e.stopPropagation();
      toggleFavorite(serverId);
      if (onToggled) onToggled();
    },
  });
  return inertToRow(btn);
}

/** The alert-count badge for a server, or null when it has nothing to show or is acknowledged. A click acknowledges. */
export function alertBadge(serverId) {
  const a = attentionFor(serverId);
  if (!a.show) return null;
  const btn = el("button", {
    type: "button",
    class: "alert-badge " + (a.critical ? "crit" : "warn"),
    title: "Click to acknowledge: hides this count in this browser until a newer alert arrives",
    "aria-label": a.count + " alerts, click to acknowledge",
    text: a.count > 99 ? "99+" : String(a.count),
    onClick: (e) => {
      e.stopPropagation();
      acknowledge(serverId);
    },
  });
  return inertToRow(btn);
}

/** The sidebar collapse button; applies the stored state to the shell at once and keeps it in step. */
export function initSidebarCollapse(appRoot, button) {
  if (!appRoot || !button) return;
  const sync = () => {
    const c = isSidebarCollapsed();
    appRoot.classList.toggle("sidebar-collapsed", c);
    button.setAttribute("aria-expanded", c ? "false" : "true");
    button.setAttribute("title", c ? "Show the sidebar" : "Hide the sidebar");
    button.setAttribute("aria-label", c ? "Show the sidebar" : "Hide the sidebar");
    button.textContent = c ? "\u00bb" : "\u00ab";
  };
  button.addEventListener("click", () => setSidebarCollapsed(!isSidebarCollapsed()));
  onChange(sync);
  sync();
}

/** The Alert Severity Colors settings: one colour input per severity and a reset, in a disclosure in the sidebar. */
export function initSeverityColorSettings(host) {
  if (!host) return;
  applySeverityColors(document.documentElement.style);
  const rebuild = () => {
    applySeverityColors(document.documentElement.style);
    mount(host, [
      el("details", { class: "viewer-settings" }, [
        el("summary", { text: "Alert severity colors" }),
        ...SEVERITIES.map((sev) => {
          const input = el("input", {
            type: "color",
            class: "sev-color-input",
            value: severityColor(sev) || DEFAULT_SEVERITY_COLORS[sev],
            "aria-label": sev + " color",
          });
          input.addEventListener("change", () => setSeverityColor(sev, input.value));
          return el("label", { class: "sev-color-row" }, [el("span", { text: sev }), input]);
        }),
        el("button", { type: "button", class: "sev-color-reset", text: "Reset to defaults", onClick: () => resetSeverityColors() }),
      ]),
    ]);
  };
  /* A colour change re-applies the properties but must not rebuild the open disclosure under the picker. */
  onChange(() => applySeverityColors(document.documentElement.style));
  rebuild();
}
