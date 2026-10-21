/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The phone-width menu (#6315). Below NARROW_QUERY the sidebar leaves the page flow (css/app.css) and the top bar's menu
 * button opens it as an overlay. This module owns the open state: the #app.nav-open class, the button's aria-expanded and
 * label, focus into the menu on open and back to the button on close, and the closes (Escape, the backdrop, a page or
 * server chosen, a route change, or the window growing past the breakpoint). At wider widths the bar is hidden and the
 * sidebar behaves as it always did, so none of this shows.
 */

export const NARROW_QUERY = "(max-width: 768px)";

/** Wires the menu button; a missing element or no matchMedia leaves the page as it was. */
export function initNarrowNav(appRoot, button, sidebar) {
  if (!appRoot || !button || !sidebar || typeof window.matchMedia !== "function") return;
  const mq = window.matchMedia(NARROW_QUERY);
  const main = document.getElementById("main");
  const backdrop = document.getElementById("nav-backdrop");

  const isOpen = () => appRoot.classList.contains("nav-open");
  const setOpen = (open, moveFocus) => {
    open = open && mq.matches;
    if (open === isOpen()) return;
    const hadFocusInside = sidebar.contains(document.activeElement);
    appRoot.classList.toggle("nav-open", open);
    button.setAttribute("aria-expanded", open ? "true" : "false");
    const label = open ? "Close the menu" : "Open the menu";
    button.setAttribute("aria-label", label);
    button.setAttribute("title", label);
    /* The page behind an open menu is not reachable by Tab or a screen reader's virtual cursor. */
    if (main) main.inert = open;
    if (open) {
      const first = sidebar.querySelector(".nav a:not([hidden])") || sidebar;
      if (first === sidebar) sidebar.setAttribute("tabindex", "-1");
      first.focus();
    } else if (moveFocus || hadFocusInside) {
      button.focus();
    }
  };

  button.addEventListener("click", () => setOpen(!isOpen(), true));
  if (backdrop) backdrop.addEventListener("click", () => setOpen(false, true));
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && isOpen()) {
      e.preventDefault();
      setOpen(false, true);
    }
  });
  /* A page link, a server row or a saved view chosen closes the menu; the route change covers every other way in. */
  sidebar.addEventListener("click", (e) => {
    if (isOpen() && e.target instanceof Element && e.target.closest("a[href], .server-item, .view-item")) setOpen(false, true);
  });
  window.addEventListener("hashchange", () => setOpen(false, false));
  const onWidth = () => {
    if (!mq.matches) setOpen(false, false);
  };
  if (typeof mq.addEventListener === "function") mq.addEventListener("change", onWidth);
  else if (typeof mq.addListener === "function") mq.addListener(onWidth);
}
