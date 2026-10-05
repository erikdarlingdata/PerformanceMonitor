/* #4666: how long until a page refreshes again. A page whose last render took more than half its interval is
   pushed out to at least 4x that render time, capped at 15 minutes, so a slow page never refreshes back to back.
   Pure: no DOM, no timers, so the rule can be run and pinned on its own. */

/** The per-page auto-refresh choices a saved definition's `refresh` key may carry, in milliseconds (0 = off). */
export const REFRESH_CHOICES = { off: 0, "1m": 60000, "5m": 300000, "15m": 900000 };

/* The built-in server and FinOps pages' interval choices (the desktop Viewer's 30s / 1m / 5m, plus Off), in the
   order the selector lists them. Saved views keep REFRESH_CHOICES: their stored `refresh` key is validated server-side. */
export const PAGE_REFRESH_CHOICES = { "30s": 30000, "1m": 60000, "5m": 300000, off: 0 };

/** The page choice before the operator picks one: the cadence these pages always ran at. */
export const DEFAULT_PAGE_REFRESH = "1m";

/** The localStorage key holding the operator's server/FinOps page choice. */
export const PAGE_REFRESH_KEY = "darling.pageRefresh";

/** `raw` when it names a page choice, else the default. */
export function pageRefreshChoiceOf(raw) {
  return typeof raw === "string" && Object.prototype.hasOwnProperty.call(PAGE_REFRESH_CHOICES, raw) ? raw : DEFAULT_PAGE_REFRESH;
}

/** The persisted page choice from a Storage-like object; unreadable storage reads as the default. */
export function loadPageRefreshChoice(storage) {
  try {
    return pageRefreshChoiceOf(storage.getItem(PAGE_REFRESH_KEY));
  } catch {
    return DEFAULT_PAGE_REFRESH;
  }
}

/** Persists the page choice and returns the validated key; storage that refuses the write still returns it. */
export function savePageRefreshChoice(storage, choice) {
  const key = pageRefreshChoiceOf(choice);
  try {
    storage.setItem(PAGE_REFRESH_KEY, key);
  } catch {
    /* private mode or a full quota: the choice holds for this page load only */
  }
  return key;
}

export const MAX_BACKOFF_MS = 15 * 60 * 1000;

/** Delay until the next page refresh, or null when auto-refresh is off for the page. */
export function nextRefreshDelayMs(intervalMs, lastRenderMs) {
  if (!(intervalMs > 0)) return null;
  if (lastRenderMs > intervalMs / 2) {
    return Math.min(Math.max(intervalMs, 4 * lastRenderMs), Math.max(intervalMs, MAX_BACKOFF_MS));
  }
  return intervalMs;
}

/** True when the last render was slow enough to push the next refresh out. */
export function isBackedOff(intervalMs, lastRenderMs) {
  return intervalMs > 0 && lastRenderMs > intervalMs / 2;
}

/** The default `refresh` choice for a saved definition that carries none: notebooks Off, dashboards 5 min. */
export function defaultRefreshChoice(isNotebook) {
  return isNotebook ? "off" : "5m";
}

/** A definition's effective `refresh` choice key (its own when valid, else the type default). */
export function refreshChoiceOf(def, isNotebook) {
  const own = def && typeof def.refresh === "string" ? def.refresh : null;
  return own !== null && Object.prototype.hasOwnProperty.call(REFRESH_CHOICES, own) ? own : defaultRefreshChoice(isNotebook);
}

/** Human label for a choice key. */
export function refreshLabel(choice) {
  return choice === "off" ? "Off" : choice === "30s" ? "30 sec" : choice === "1m" ? "1 min" : choice === "5m" ? "5 min" : "15 min";
}
