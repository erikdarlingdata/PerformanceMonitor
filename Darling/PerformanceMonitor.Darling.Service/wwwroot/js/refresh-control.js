/* #4666: the labelled "Auto-refresh:" select shown on a saved view/notebook page and in both editors. */
import { el } from "./util.js";
import { REFRESH_CHOICES, PAGE_REFRESH_CHOICES, refreshLabel } from "./refresh-policy.js";

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

/**
 * The shell's interval selector plus Refresh button for the built-in server and FinOps pages. It lives outside
 * #main, so the page's own rebuild never resets it.
 * @param {string} current  the effective page choice key ("30s" | "1m" | "5m" | "off")
 * @param {(choice: string) => void} onChange  called with the new key when the operator picks one
 * @param {() => void} onRefresh  called once per Refresh click
 * @returns {{ root: HTMLElement, select: HTMLElement, setBusy: (busy: boolean) => void, setChoice: (choice: string) => void }}
 *   `setBusy` disables the Refresh button (aria-busy, "Refreshing…" title) while reads run; `setChoice` shows a choice
 *   another tab saved.
 */
export function buildPageRefreshControl(current, onChange, onRefresh) {
  const select = el("select", { class: "refresh-select", id: "page-refresh-select", "aria-label": "Auto-refresh interval",
    title: "How often this page reloads its data. Off pauses this page only; the sidebar and status bar keep updating." });
  for (const key of Object.keys(PAGE_REFRESH_CHOICES)) {
    select.appendChild(el("option", { value: key, text: refreshLabel(key) }));
  }
  select.value = current;
  select.addEventListener("change", () => onChange(select.value));
  const button = el("button", { class: "auto-refresh-toggle", id: "page-refresh-now", type: "button", text: "Refresh" });
  button.addEventListener("click", () => onRefresh());
  const idleTitle = "Reload this page now";
  button.setAttribute("title", idleTitle);
  const setBusy = (busy) => {
    button.disabled = busy;
    if (busy) button.setAttribute("aria-busy", "true"); else button.removeAttribute("aria-busy");
    button.setAttribute("title", busy ? "Refreshing…" : idleTitle);
  };
  const setChoice = (choice) => { select.value = choice; };
  const root = el("div", { class: "refresh-control page-refresh-control", id: "page-refresh-control" }, [
    el("span", { text: "Auto-refresh:" }), select, button,
  ]);
  return { root, select, setBusy, setChoice };
}
