/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The #3285 custom-alert-rule API client (Component 7) — the twin of views-api.js, thin wrappers over util.js's
 * apiGet/apiSend for the seven /api/alerts endpoints. The rule CRUD mirrors the view CRUD EXACTLY: a bare-array
 * list of summaries, a full single rule with its embedded definition, create (POST -> 201/400/409), a full-replace
 * update (PUT with `version` for optimistic concurrency -> 200/400/404/409), and delete (204/404). Two alert-only
 * POSTs sit beside them: /validate dry-runs a definition ({valid,error}) and /test evaluates it now (per-server
 * current value + would-fire). The backend re-validates every write and the /validate + /test verdicts as the
 * authority — this client only shapes the requests and forwards the classified response.
 *
 * The session ({can_edit}) and read catalog are the SAME per-page-load resources the composer uses, so they are
 * re-exported from views-api.js rather than re-fetched: one cached promise backs the views composer, the notebook
 * composer, and this editor alike (see the "maximize shared base code" rule). can_edit is the server's answer for
 * this seat; the UI hides every edit affordance when it is false, and the server's method-based write gate refuses
 * a viewer-seat mutation regardless (this is the affordance layer, never the enforcement).
 */

import { apiGet, apiSend } from "./util.js";

/* Shared session + catalog — one cache for every composer surface (re-exported, not re-implemented). */
export { getSession, getCatalog } from "./views-api.js";

/** GET the bare-array list of rule summaries (no definition), each carrying `enabled`. */
export function listAlertRules() {
  return apiGet("/api/alerts");
}

/** GET one rule in full (with its embedded definition and `enabled`). */
export function getAlertRule(id) {
  return apiGet("/api/alerts/" + encodeURIComponent(id));
}

/** POST a new rule {name, description?, definition, enabled?} — 201 + the full rule, or 400/409(dup)/403. */
export function createAlertRule(body) {
  return apiSend("POST", "/api/alerts", body);
}

/** PUT an existing rule {name, description?, definition, enabled?, version} — 200 + the full rule, or 400/404/409/403. */
export function updateAlertRule(id, body) {
  return apiSend("PUT", "/api/alerts/" + encodeURIComponent(id), body);
}

/** DELETE a rule — 204, or 404/403. (An open incident is force-resolved server-side before the delete, #3305.) */
export function deleteAlertRule(id) {
  return apiSend("DELETE", "/api/alerts/" + encodeURIComponent(id));
}

/** GET the built-in starter templates — {templates:[{key,name,description,definition}]}; server-authored (#3325). */
export function listAlertTemplates() {
  return apiGet("/api/alert-templates");
}

/** POST a dry-run validation of a definition — {valid, error}; the SAME authority the write path applies. */
export function validateAlertRule(definition) {
  return apiSend("POST", "/api/alerts/validate", { definition });
}

/**
 * POST an evaluate-now test — read the metric's CURRENT value on each in-scope server and report whether it WOULD
 * breach right now (delivering/persisting nothing). Pass exactly one of {rule_id} (a saved rule) or {definition}
 * (an unsaved draft). Returns {rule_id,name,note,results:[{server,current_value,breaching,severity,no_data}]} as
 * data; an invalid/not-found draft comes back as the {status,message} envelope (classified "empty"); a valid draft
 * with no matching servers as data carrying status "no_in_scope_servers".
 */
export function testAlertRule(body) {
  return apiSend("POST", "/api/alerts/test", body);
}
