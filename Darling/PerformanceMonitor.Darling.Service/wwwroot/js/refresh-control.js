/* #4666: the labelled "Auto-refresh:" select shown on a saved view/notebook page and in both editors. */
import { el } from "./util.js";
import { REFRESH_CHOICES, refreshLabel } from "./refresh-policy.js";

/**
 * @param {string} current  the effective choice key ("off" | "1m" | "5m" | "15m")
 * @param {(choice: string) => void} onChange  called with the new key when the operator picks one
 * @returns {{ root: HTMLElement, note: (text: string) => void }}  `note` sets the small text beside the select
 */
export function buildRefreshControl(current, onChange) {
  const select = el("select", { class: "refresh-select", id: "view-refresh-select", "aria-label": "Auto-refresh" });
  for (const key of Object.keys(REFRESH_CHOICES)) {
    select.appendChild(el("option", { value: key, text: refreshLabel(key) }));
  }
  select.value = current;
  const noteEl = el("span", { class: "meta refresh-note" });
  select.addEventListener("change", () => onChange(select.value));
  const root = el("label", { class: "refresh-control" }, [el("span", { text: "Auto-refresh:" }), select, noteEl]);
  return { root, note: (text) => { noteEl.textContent = text || ""; } };
}
