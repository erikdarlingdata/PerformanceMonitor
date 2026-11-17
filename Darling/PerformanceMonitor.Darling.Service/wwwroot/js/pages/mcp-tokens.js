/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * My MCP tokens (#8737): the page where a person signed in with OpenID Connect makes, lists and revokes their own MCP tokens, and where an
 * administrator sees everyone's. It reads GET /api/mcp-tokens ({tokens, max_lifetime_days, allowed_roles}; ?all=1 adds each row's subject and
 * is for an admin seat) and writes POST /api/mcp-tokens (the create, the one answer that carries the token's value) and
 * POST /api/mcp-tokens/{id}/revoke. The server is the authority for every limit; the form only offers what the server just said it allows.
 *
 * The page sends nothing on load but reads. The token value lives in module scope only until the page is drawn again (a visit starts a
 * fresh draw, and this route is never re-drawn by the poll), so leaving the page drops it. Every value is drawn through el() as text.
 */

import { el, mount, apiGet, apiSend, apiWrite, localTime, loadingStrip, errorStrip, emptyStrip } from "../util.js";

const ROUTE = "/api/mcp-tokens";
const COPY_NOTE = "This is the only time this token is shown. Copy it now; it cannot be shown again.";

/* The value just created, or null. Reset by every fresh draw of the page, so it never outlives a visit. */
let shown = null;

function messageOf(result, fallback) {
  if (result.body && typeof result.body.message === "string" && result.body.message) return result.body.message;
  if (result.message) return result.message;
  return fallback + " (HTTP " + result.status + ").";
}

/** The State cell: the state, and for a revoked row who revoked it and why. */
export function stateText(row) {
  if (row.state !== "revoked") return String(row.state || "");
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
    const copy = el("button", { class: "btn", type: "button", text: "Copy" });
    copy.addEventListener("click", async () => {
      try {
        await navigator.clipboard.writeText(shown.token);
        copy.textContent = "Copied";
      } catch (e) {
        copy.textContent = "Copy failed: select the value and copy it by hand";
      }
    });
    mount(valueHost, el("div", { class: "token-value-panel card", role: "status" }, [
      el("div", { class: "token-value-name", text: shown.record.name }),
      el("div", { class: "token-value-note", text: COPY_NOTE }),
      el("code", { class: "token-value", text: shown.token }),
      copy,
    ]));
  };

  const revoke = async (row) => {
    if (busy) return;
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
    mount(target, res.data.tokens.length === 0 ? emptyStrip(emptyText) : tokenTable(res.data.tokens, withSubject, revoke));
  };

  const drawForm = () => {
    const name = el("input", { type: "text", class: "mcp-token-input", "aria-label": "Token name", maxlength: "64", autocomplete: "off" });
    const days = el("input", { type: "number", class: "mcp-token-input", "aria-label": "Lifetime in days", min: "1", max: String(policy.maxLifetimeDays), value: String(policy.maxLifetimeDays) });
    const role = el("select", { class: "mcp-token-input", "aria-label": "Role" }, policy.allowedRoles.map((r) => el("option", { value: r, text: r })));
    const create = el("button", { class: "btn primary", type: "button", text: "Create token" });
    create.addEventListener("click", async () => {
      if (busy) return;
      busy = true;
      say(null);
      const res = await apiWrite("POST", ROUTE, { name: String(name.value || "").trim(), lifetime_days: Number.parseInt(days.value, 10) || 0, role: role.value });
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

  mount(ownHost, loadingStrip("Loading your tokens…"));
  mount(everyoneHost, loadingStrip("Loading every token…"));
  return load();
}
