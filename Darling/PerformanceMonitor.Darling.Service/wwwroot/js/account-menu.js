/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The account menu (#8737): one line in the sidebar's top block (and in the top bar at narrow widths) that says who this session
 * is, and opens a popover with the details. Everything comes from GET /api/me. The subject, display name, token name and group
 * name are the signed-in party's own text, so they reach the page only through el() (text nodes, never markup). Nothing here
 * authorizes anything: the page only describes the session the server already holds.
 */

import { el, mount, apiGet, localTime } from "./util.js";

const SEAT_WORDS = { admin: "admin", read_only: "read-only" };
const METHOD_WORDS = { oidc: "OpenID Connect", shared_token: "shared token", named_token: "web token", managed_group: "access group" };

function seatWord(me) {
  return SEAT_WORDS[me && me.seat] || String((me && me.seat) || "");
}

/** The one line the control shows: who, and with which seat. */
export function accountLine(me) {
  const seat = seatWord(me);
  switch (me && me.method) {
    case "oidc": return (me.subject || "signed in") + " · " + seat;
    case "shared_token": return "shared token · " + seat;
    case "named_token": return (me.token_name ? "token " + me.token_name : "web token") + " · " + seat;
    case "managed_group": return (me.group_name ? "group " + me.group_name : "access group") + " · " + seat;
    case "reads_only": return "read-only access";
    default: return seat || "signed in";
  }
}

/** The popover's rows, as [label, value] pairs, in the order they are drawn. */
export function accountDetails(me) {
  const rows = [];
  const oidc = me.method === "oidc";
  if (oidc && me.display_name) rows.push(["Name", me.display_name]);
  if (oidc) rows.push(["Signed in as", me.subject || ""]);
  const seat = seatWord(me);
  /* A null granted_by is not "no mapping": a sign-in with no owner key carries no grant at all. The server says it with role_mapping. */
  if (oidc) rows.push(["Seat", seat + (me.granted_by ? " (granted by " + me.granted_by + ")" : me.role_mapping === false ? " (no role mapping set)" : "")]);
  else rows.push(["Seat", seat]);
  if (METHOD_WORDS[me.method]) rows.push(["Signed in with", METHOD_WORDS[me.method]]);
  if (me.expires_at) rows.push(["Session ends", localTime(me.expires_at)]);
  return rows;
}

/** The links the popover offers besides its rows, as {text, href}. The personal-token page is offered only where the host serves it for this sign-in. */
export function accountLinks(me) {
  return me && me.personal_tokens === true ? [{ text: "My MCP tokens", href: "#/mcp-tokens" }] : [];
}

let meRead = null;

/** The /api/me answer, read once and shared by the menu and the personal-token page; null when there is no data answer (and the next call asks again). */
export function getMe() {
  if (!meRead) {
    meRead = apiGet("/api/me").then((res) => (res.kind === "data" && res.data && typeof res.data === "object" ? res.data : null));
    meRead.then((me) => {
      if (!me) meRead = null;
    });
  }
  return meRead;
}

let panelCount = 0;

/** One control: the button with the line, and (when open) the panel it discloses. Each mount gets its own, with its own open state
 *  and panel id. A disclosure, not a menu: the panel is information rows and plain links (My MCP tokens when the host serves them, and
 *  Sign out for a session that expires). */
function buildControl(me) {
  const panelId = "account-panel-" + ++panelCount;
  const button = el("button", { class: "account-btn", type: "button", "aria-expanded": "false", "aria-controls": panelId }, [
    el("span", { text: accountLine(me) }),
    el("span", { class: "account-caret", "aria-hidden": "true", text: " ▾" }),
  ]);
  const root = el("div", { class: "account-control" }, [button]);
  const signOut = me.expires_at ? el("a", { class: "account-item", href: "/auth/logout", text: "Sign out" }) : null;
  let popup = null;
  let outside = null;
  let escape = null;

  const close = () => {
    if (popup) {
      root.removeChild(popup);
      popup = null;
    }
    button.setAttribute("aria-expanded", "false");
    if (outside) document.removeEventListener("click", outside);
    outside = null;
    if (escape) document.removeEventListener("keydown", escape);
    escape = null;
  };
  const closeToButton = () => {
    close();
    button.focus();
  };
  const open = () => {
    const rows = accountDetails(me).map(([label, value]) =>
      el("div", { class: "account-row" }, [el("span", { class: "account-label", text: label }), el("span", { class: "account-value", text: value })])
    );
    const links = accountLinks(me).map((l) => el("a", { class: "account-item", href: l.href, text: l.text, onClick: close }));
    popup = el("div", { id: panelId, class: "account-pop" }, [...rows, ...links, signOut]);
    root.appendChild(popup);
    button.setAttribute("aria-expanded", "true");
    outside = (e) => {
      if (e && e.target && root.contains(e.target)) return;
      close();
    };
    /* After this click finishes, so the click that opened the popover does not close it. */
    setTimeout(() => outside && document.addEventListener("click", outside), 0);
    /* On the document, so Escape works when the button never took focus (Safari does not focus a clicked button). */
    escape = (e) => {
      if (e.key === "Escape") {
        e.preventDefault();
        closeToButton();
      }
    };
    document.addEventListener("keydown", escape);
  };
  button.addEventListener("click", () => (popup ? close() : open()));
  /* Tab follows the browser's own order; the panel closes when focus leaves the whole control, and focus is left where it went. */
  root.addEventListener("focusout", (e) => {
    if (popup && e.relatedTarget && !root.contains(e.relatedTarget)) close();
  });
  return root;
}

/**
 * Reads /api/me once and draws the control into both hosts (the sidebar's and the top bar's; the stylesheet shows one of them by
 * width). Any answer but data leaves both hosts hidden: a page with no session answer shows no account line.
 */
export async function initAccountMenu(brandHost, topbarHost) {
  const me = await getMe();
  if (!me) return;
  for (const host of [brandHost, topbarHost]) {
    if (!host) continue;
    mount(host, buildControl(me));
    host.hidden = false;
  }
}
