/* #4666: how long until a page refreshes again. A page whose last render took more than half its interval is
   pushed out to at least 4x that render time, capped at 15 minutes, so a slow page never refreshes back to back.
   Pure: no DOM, no timers, so the rule can be run and pinned on its own. */

/** The per-page auto-refresh choices a saved definition's `refresh` key may carry, in milliseconds (0 = off). */
export const REFRESH_CHOICES = { off: 0, "1m": 60000, "5m": 300000, "15m": 900000 };

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
  return choice === "off" ? "Off" : choice === "1m" ? "1 min" : choice === "5m" ? "5 min" : "15 min";
}
