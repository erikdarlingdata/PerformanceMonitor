/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* Admin > Notification Routes: the Slack bot's tokens (#7656). The bot token and the app-level token live on the notification settings
   row beside the Slack webhook URL and are sealed by the service the same way: the page never receives one, a box is for a NEW value,
   Remove deletes the saved one, and a box left blank keeps it.
     PUT /api/slack-bot-tokens   {slack_bot_token?, slack_app_token?}; "-" removes; answers {status, secrets: {field: {before, after}}}
   Shown to the same seat that edits routes. Self-contained: admin.js mounts the nodes this file builds. */

import { el, errorStrip, noticeStrip, captionStrip, apiWrite } from "../util.js";

/** The two tokens: the body field, the entry key, the Remove key, the label and the prefix Slack gives each. */
export const SLACK_BOT_FIELDS = [
  { field: "slack_bot_token", key: "bot", clearKey: "clearBot", label: "Slack bot token", prefix: "xoxb-" },
  { field: "slack_app_token", key: "app", clearKey: "clearApp", label: "Slack app-level token", prefix: "xapp-" },
];

export const SLACK_BOT_NOTE =
  "The Slack bot posts each investigation request into one thread. The bot token starts with xoxb- and the app-level token with xapp-; " +
  "both are stored sealed and never shown again. A box left blank keeps the saved token, and Remove deletes it. The route form's " +
  "Slack bot channel field lists the scopes the app needs.";

/** The body to send: a typed token under its field, "-" for one marked Remove, nothing for a box left alone (the service keeps it). */
export function slackBotPayload(entries) {
  const out = {};
  for (const f of SLACK_BOT_FIELDS) {
    const typed = String((entries && entries[f.key]) || "").trim();
    if (typed !== "") out[f.field] = typed;
    else if (entries && entries[f.clearKey] === true) out[f.field] = "-";
  }
  return out;
}

/** What a write answer says, in the page's own words: { message, isError }. A token is said as set or not set, never as a value, whatever
    the answer carried. An expired session says nothing here. */
export function slackBotOutcome(res) {
  if (!res || res.expired) return { message: null, isError: true };
  if (res.status === 0) return { message: res.message || "Network error.", isError: true };
  const body = res.body && typeof res.body === "object" ? res.body : null;
  if (res.status === 200 && body && body.status === "unchanged") return { message: "Nothing differs from the saved Slack bot tokens.", isError: false };
  if (res.status === 200 && body && body.status === "updated") {
    const secrets = body.secrets && typeof body.secrets === "object" ? body.secrets : {};
    const parts = SLACK_BOT_FIELDS.filter((f) => secrets[f.field] && typeof secrets[f.field] === "object").map((f) => {
      const after = secrets[f.field].after;
      return f.label + ": " + (after === "not set" || !after ? "not set" : "set") + ".";
    });
    return { message: parts.length ? parts.join(" ") : "Slack bot tokens saved.", isError: false };
  }
  const said = body && typeof body.error === "string" && body.error ? body.error : "";
  if (res.status === 403) return { message: said || "This sign-in cannot change the Slack bot tokens.", isError: true };
  return { message: "The Slack bot tokens were not saved." + (said ? " " + said : " The service answered " + res.status + "."), isError: true };
}

let entries = null;
let status = null;
let busy = false;

/** The nodes the Routes tab shows for an editing seat: the last write's outcome, the note and the form. */
export function slackBotTokenNodes(options) {
  const nodes = [];
  if (status) nodes.push(status.isError ? errorStrip(status.message) : noticeStrip(status.message));
  nodes.push(captionStrip(SLACK_BOT_NOTE));
  nodes.push(form(options));
  return nodes;
}

function form(options) {
  const e = entries || { bot: "", app: "", clearBot: false, clearApp: false };
  const boxes = {};
  const removes = {};
  const fields = SLACK_BOT_FIELDS.map((f) => {
    const box = el("input", { type: "password", class: "tag-input", autocomplete: "off", "data-field": f.key + "Token", placeholder: "new value, " + f.prefix });
    box.value = e[f.key] || "";
    boxes[f.key] = box;
    const remove = el("input", { type: "checkbox", "data-field": f.key + "Remove" });
    remove.checked = e[f.clearKey] === true;
    removes[f.key] = remove;
    return el("label", { class: "tag-field" }, [el("span", { class: "mute-label", text: f.label + ":" }), box, remove, el("span", { text: " Remove" })]);
  });
  const read = () => {
    const out = {};
    for (const f of SLACK_BOT_FIELDS) {
      out[f.key] = boxes[f.key].value;
      out[f.clearKey] = removes[f.key].checked;
    }
    return out;
  };
  const sync = () => { entries = read(); };
  for (const c of [...Object.values(boxes), ...Object.values(removes)]) c.addEventListener("input", sync);
  const save = el("button", {
    class: "btn primary", type: "button", "data-action": "save-slack-bot-tokens", text: "Save Slack bot tokens",
    onClick: async () => {
      if (busy) return;
      busy = true;
      let out;
      try {
        const payload = slackBotPayload(read());
        out = Object.keys(payload).length === 0
          ? { message: "Type a token, or mark one Remove.", isError: true }
          : slackBotOutcome(await apiWrite("PUT", "/api/slack-bot-tokens", payload));
      } catch (err) {
        out = { message: "Could not save the Slack bot tokens: " + (err && err.message ? err.message : String(err)), isError: true };
      }
      busy = false;
      if (out.message) status = { message: out.message, isError: out.isError };
      if (out.isError === false) entries = null;
      await options.repaint();
    },
  });
  return el("div", { class: "tag-form", "data-slack-bot-form": "1" }, [...fields, save]);
}
