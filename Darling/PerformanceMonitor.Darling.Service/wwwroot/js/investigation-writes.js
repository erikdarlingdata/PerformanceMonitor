/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* The three investigation writes a person makes from the web (#6265), the twins of the desktop Viewer's: close a request
   (PUT /api/investigation-requests/{id}/close), write a note (PUT /api/investigation-notes) and start a practice request
   (PUT /api/investigation-practice-requests/{id}). This module holds the part the three screens share: the paths, the bodies
   and the plain-words answer. The service checks every value again, and a store login without the admin role answers 403 with
   the sentence that says what to do. */

import { apiWrite } from "./util.js";

export const NOTE_MAX_LENGTH = 2000;
export const CLOSE_REASON_MAX_LENGTH = 500;
export const NOTE_DEFAULT_DAYS = 90;
export const NOTE_MAX_DAYS = 365;

/** What the web says when a change needs a signed-in admin: a dashboard with no sign-in configured has no caller to name. The service
    answers a change with the same sentence (403) and writes nothing. */
export const SIGN_IN_SENTENCE = "Sign in to the web as an admin to make this change, or use the Viewer.";

/** Whether this session may make the person-made investigation changes: "yes" for a signed-in admin, "sign-in" for a seat that can edit
    but is not signed in (the pages list only and say so), "no" for a read-only seat. */
export function writeAccess(session) {
  if (!session || session.can_edit !== true) return "no";
  return session.signed_in_admin === true ? "yes" : "sign-in";
}

/** The states a request can be closed from. */
export function canCloseState(state) {
  return state === "open" || state === "claimed" || state === "reported";
}

/** What an answer says, in plain words: { ok, message }. `ok` is true on a 200 whose body carries `status` = `expected`. An expired
    session says nothing here (the shell has already told the person). The service's own sentence is shown for a refusal. */
export function writeOutcome(res, expected, success, failed) {
  if (!res || res.expired) return { ok: false, message: null };
  if (res.status === 0) return { ok: false, message: res.message || "Network error." };
  const body = res.body && typeof res.body === "object" ? res.body : null;
  if (res.status === 200 && body && body.status === expected) return { ok: true, message: success(body) };
  const said = body && typeof body.error === "string" && body.error ? body.error : "The service answered " + res.status + ".";
  return { ok: false, message: failed + " " + said };
}

export async function closeRequest(requestId, reason) {
  const res = await apiWrite("PUT", "/api/investigation-requests/" + encodeURIComponent(requestId) + "/close", { reason });
  return writeOutcome(res, "closed", () => "Request closed.", "The request was not closed.");
}

/** The body of a note write from the form's values: a blank server is the fleet, a blank metric is none, blank days is the default. */
export function buildNoteBody(values) {
  const body = { note_text: typeof values.note_text === "string" ? values.note_text : "" };
  if (values.server_id !== undefined && values.server_id !== null && values.server_id !== "") body.server_id = Number(values.server_id);
  if (typeof values.metric_name === "string" && values.metric_name.trim() !== "") body.metric_name = values.metric_name;
  if (values.days !== undefined && values.days !== null && String(values.days).trim() !== "") body.days = Number(values.days);
  return body;
}

/** What is missing from a note before the service is asked, or null: the same sentence the service answers a blank note with, so an
    empty submit shows it at once instead of after a request that is refused (and logs a 400 in the console). */
export function noteMissing(values) {
  const text = values && typeof values.note_text === "string" ? values.note_text : "";
  return text.trim() === "" ? "Enter the note's text." : null;
}

export async function writeNote(values) {
  const missing = noteMissing(values);
  if (missing) return { ok: false, message: "The note was not written. " + missing };
  const res = await apiWrite("PUT", "/api/investigation-notes", buildNoteBody(values));
  return writeOutcome(res, "approved", (b) => "Note " + b.note_id + " written. It is approved and goes into the briefs for its scope.", "The note was not written.");
}

export async function startPractice(requestId) {
  const res = await apiWrite("PUT", "/api/investigation-practice-requests/" + encodeURIComponent(requestId), {});
  return writeOutcome(
    res, "opened",
    (b) => "Practice request opened (" + b.request_id + "). An agent on the case's route can claim it; its report is scored when it arrives.",
    "The practice request was not opened.");
}
