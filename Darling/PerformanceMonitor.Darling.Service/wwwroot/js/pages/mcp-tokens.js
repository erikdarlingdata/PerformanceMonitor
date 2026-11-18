/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * My MCP tokens (#8737): the page where a person signed in with OpenID Connect makes, lists and revokes their own MCP tokens, and where an
 * administrator sees everyone's. It reads GET /api/mcp-tokens ({tokens, truncated, max_lifetime_days, allowed_roles}; ?all=1 adds each row's subject and
 * is for an admin seat) and writes POST /api/mcp-tokens (the create, the one answer that carries the token's value) and
 * POST /api/mcp-tokens/{id}/revoke. The server is the authority for every limit; the form only offers what the server just said it allows.
 *
 * The page sends nothing on load but reads. The token value lives in module scope only until the page is drawn again (a visit starts a
 * fresh draw, and this route is never re-drawn by the poll), so leaving the page drops it. Every value is drawn through el() as text.
 */

import { el, mount, apiGet, apiSend, apiWrite, localTime, loadingStrip, errorStrip, emptyStrip, noticeStrip } from "../util.js";

const ROUTE = "/api/mcp-tokens";
const COPY_NOTE = "This is the only time this token is shown. Copy it now; it cannot be shown again.";

/* The value just created, or null. Reset by every fresh draw of the page, so it never outlives a visit. */
let shown = null;
/* Clears the value of the page now drawn, when the address changes. Registered once, below. */
let leave = null;
window.addEventListener("hashchange", () => {
  if (leave) leave();
});

function messageOf(result, fallback) {
  if (result.body && typeof result.body.message === "string" && result.body.message) return result.body.message;
  if (result.message) return result.message;
  return fallback + " (HTTP " + result.status + ").";
}

/* The sentence for each revoke reason code the server stores. A code not listed here is shown as it is, with who revoked it. */
const REVOKE_REASONS = {
  revoked_by_owner: () => "revoked by its owner",
  revoked_by_admin: (row) => "revoked by an administrator" + (row.revoked_by ? " (" + row.revoked_by + ")" : ""),
  sign_in_refused: () => "revoked at sign-in: the sign-in was refused",
  seat_lowered_to_read_only: () => "revoked at sign-in: the seat is now read-only",
  operator_tokens_disabled: () => "revoked: operator tokens are turned off",
};

/** The State cell: the state, and for a revoked row who revoked it and why. */
export function stateText(row) {
  if (row.state !== "revoked") return String(row.state || "");
  const known = Object.prototype.hasOwnProperty.call(REVOKE_REASONS, row.revoke_reason) ? REVOKE_REASONS[row.revoke_reason] : null;
  if (known) return known(row);
  const why = [row.revoke_reason, row.revoked_by ? "by " + row.revoked_by : null].filter(Boolean).join(" ");
  return why ? "revoked: " + why : "revoked";
}

function tokenTable(rows, withSubject, onRevoke) {
  const heads = ["Name", ...(withSubject ? ["Subject"] : []), "Role", "Created", "Expires", "Last used", "State", ""];
  const body = rows.map((row) => {
    const cells = [
      el("td", { text: row.name }),
      ...(withSubject ? [el("td", { text: row.subject || "" })] : []),
      el("td", { text: row.role }),
      el("td", { text: localTime(row.created_at) }),
      el("td", { text: localTime(row.expires_at) }),
      el("td", { text: row.last_used_at ? localTime(row.last_used_at) : "never" }),
      el("td", { text: stateText(row) }),
      el("td", {}, [row.state === "live" ? el("button", { class: "btn", type: "button", text: "Revoke", onClick: () => onRevoke(row) }) : null]),
    ];
    return el("tr", { "data-token": row.id }, cells);
  });
  return el("div", { class: "table-wrap" }, [
    el("table", { class: "data mcp-tokens" }, [
      el("thead", {}, [el("tr", {}, heads.map((h) => el("th", { text: h })))]),
      el("tbody", {}, body),
    ]),
  ]);
}

/**
 * Draws the page into `host`. `me` is the /api/me answer: the page shows its own tokens (and the create form) to an OpenID Connect
 * sign-in, and the Everyone section to an admin seat. Any other session, or a host that serves no personal tokens, gets one sentence.
 */
export function renderMcpTokens(host, me) {
  shown = null;
  leave = null;
  if (!me || me.personal_tokens !== true) {
    mount(host, [el("h2", { text: "My MCP tokens" }), emptyStrip("Personal MCP tokens are not available for this sign-in or on this host.")]);
    return Promise.resolve();
  }
  const oidc = me.method === "oidc";
  const admin = me.seat === "admin";

  const noticeHost = el("div", {});
  const valueHost = el("div", {});
  const formHost = el("div", {});
  const ownHost = el("div", {});
  const everyoneHost = el("div", {});
  const sections = [el("div", { class: "page-head" }, [el("h2", { text: "My MCP tokens" })]), noticeHost, valueHost];
  if (oidc) {
    sections.push(formHost, el("h3", { text: "My tokens" }), ownHost);
  } else {
    sections.push(emptyStrip("Personal MCP tokens are made by a person signed in with OpenID Connect. This sign-in can only list and revoke them."));
  }
  if (admin) sections.push(el("h3", { text: "Everyone" }), everyoneHost);
  mount(host, sections);

  let policy = { maxLifetimeDays: 1, allowedRoles: [] };
  let busy = false;

  const say = (strip) => mount(noticeHost, strip ? [strip] : []);

  const drawValue = () => {
    if (!shown) {
      mount(valueHost, []);
      return;
    }
    const value = shown;
    const copy = el("button", { class: "btn", type: "button", text: "Copy" });
    copy.addEventListener("click", async () => {
      try {
        await navigator.clipboard.writeText(value.token);
        copy.textContent = "Copied";
      } catch (e) {
        copy.textContent = "Copy failed: select the value and copy it by hand";
      }
    });
    mount(valueHost, el("div", { class: "token-value-panel card", role: "status" }, [
      el("div", { class: "token-value-name", text: value.record.name }),
      el("div", { class: "token-value-note", text: COPY_NOTE }),
      el("code", { class: "token-value", text: value.token }),
      copy,
    ]));
  };

  const revoke = async (row, withSubject) => {
    if (busy) return;
    const whose = withSubject && row.subject ? " (belonging to " + row.subject + ")" : "";
    if (!window.confirm("Revoke the token \"" + row.name + "\"" + whose + "? Any MCP client using it stops working at once, and it cannot be undone.")) return;
    busy = true;
    say(null);
    const res = await apiSend("POST", ROUTE + "/" + encodeURIComponent(row.id) + "/revoke");
    busy = false;
    if (res.kind === "auth") return;
    if (res.kind === "error" && res.status !== 404) {
      say(errorStrip(res.message));
      return;
    }
    if (res.kind === "error") say(emptyStrip("That token is already revoked or gone."));
    await load();
  };

  const drawRows = (target, res, withSubject, emptyText) => {
    if (res.kind === "auth" || res.kind === "aborted") return;
    if (res.kind !== "data" || !res.data || !Array.isArray(res.data.tokens)) {
      mount(target, res.kind === "error" ? errorStrip(res.message) : emptyStrip(res.message || emptyText));
      return;
    }
    if (res.data.tokens.length === 0) {
      mount(target, emptyStrip(emptyText));
      return;
    }
    const table = tokenTable(res.data.tokens, withSubject, (row) => revoke(row, withSubject));
    mount(target, res.data.truncated === true
      ? [noticeStrip("Only " + res.data.tokens.length + " tokens are shown; expired and revoked ones are left out first, then the oldest active ones."), table]
      : table);
  };

  const drawForm = () => {
    const name = el("input", { type: "text", class: "mcp-token-input", "aria-label": "Token name", maxlength: "64", autocomplete: "off" });
    const days = el("input", { type: "number", class: "mcp-token-input", "aria-label": "Lifetime in days", min: "1", max: String(policy.maxLifetimeDays), value: String(policy.maxLifetimeDays) });
    const role = el("select", { class: "mcp-token-input", "aria-label": "Role" }, policy.allowedRoles.map((r) => el("option", { value: r, text: r })));
    const create = el("button", { class: "btn primary", type: "button", text: "Create token" });
    create.addEventListener("click", async () => {
      if (busy) return;
      say(null);
      const typed = String(days.value == null ? "" : days.value).trim();
      const lifetime = /^[0-9]+$/.test(typed) ? Number(typed) : 0;
      if (lifetime < 1 || lifetime > policy.maxLifetimeDays) {
        say(errorStrip("The lifetime must be a whole number of days from 1 to " + policy.maxLifetimeDays + "."));
        return;
      }
      busy = true;
      const res = await apiWrite("POST", ROUTE, { name: String(name.value || "").trim(), lifetime_days: lifetime, role: role.value });
      busy = false;
      if (res.expired) return;
      if (res.status === 201 && res.body && typeof res.body.token === "string" && res.body.record) {
        shown = { token: res.body.token, record: res.body.record };
        drawValue();
        name.value = "";
        await load();
        return;
      }
      say(errorStrip(messageOf(res, "The token was not created")));
    });
    mount(formHost, el("div", { class: "mcp-token-form" }, [
      el("label", { class: "mcp-token-field" }, [el("span", { text: "Name" }), name]),
      el("label", { class: "mcp-token-field" }, [el("span", { text: "Lifetime (days, up to " + policy.maxLifetimeDays + ")" }), days]),
      el("label", { class: "mcp-token-field" }, [el("span", { text: "Role" }), role]),
      create,
    ]));
  };

  const load = async () => {
    const [own, everyone] = await Promise.all([oidc ? apiGet(ROUTE) : null, admin ? apiGet(ROUTE + "?all=1") : null]);
    if (own) {
      if (own.kind === "data" && own.data && Array.isArray(own.data.allowed_roles)) {
        const first = !formHost.firstChild;
        policy = { maxLifetimeDays: Number(own.data.max_lifetime_days) || 1, allowedRoles: own.data.allowed_roles };
        if (first) drawForm();
      }
      drawRows(ownHost, own, false, "You have no tokens yet.");
    }
    if (everyone) drawRows(everyoneHost, everyone, true, "Nobody has a token yet.");
  };

  leave = () => {
    shown = null;
    mount(valueHost, []);
  };
  mount(ownHost, loadingStrip("Loading your tokens…"));
  mount(everyoneHost, loadingStrip("Loading every token…"));
  return load();
}
