/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* Copy and CSV helpers for the shared table renderer (#4843). Pure functions plus two thin browser seams
   (clipboard, download); nothing here touches the DOM tree, so each is testable under Node. */

/** A cell as tab-separated text sees it: tabs and line breaks collapse to a space so one cell stays one field. */
export function plainCell(text) {
  return String(text ?? "").replace(/[\t\r\n]+/g, " ");
}

/** Rows of already-formatted cells as tab-separated lines (the form a spreadsheet pastes into columns). */
export function toTsv(rows) {
  return rows.map((r) => r.map(plainCell).join("\t")).join("\n");
}

/* A spreadsheet runs a cell that starts with one of these as a formula. A leading apostrophe makes it text. A real
   number is left alone (a negative value is data, not a formula), and so is a string that is only a number
   ("-5", "+3", "1e-3"); any other string is neutralised. */
const FORMULA_LEAD = /^[=+\-@\t\r]/;
const PLAIN_NUMBER = /^[+-]?\d+(\.\d+)?([eE][+-]?\d+)?$/;

/** One CSV field, RFC 4180: quoted when it holds a comma, quote, CR or LF, with quotes doubled. */
export function csvField(v) {
  if (v == null) return "";
  let s = v instanceof Date ? v.toISOString() : String(v);
  if (typeof v === "string" && FORMULA_LEAD.test(s) && !PLAIN_NUMBER.test(s)) s = "'" + s;
  return /[",\r\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
}

/** Rows (arrays of raw values) as CSV text, CRLF-separated per RFC 4180. */
export function toCsv(rows) {
  return rows.map((r) => r.map(csvField).join(",")).join("\r\n");
}

/** True for a value a CSV cell cannot hold as one scalar: an array or a plain object (a Date is a scalar). */
export function isListValue(v) {
  return v != null && typeof v === "object" && !(v instanceof Date);
}

/** A file name from the table's title and the time: "slow-queries-20260105-143007.csv" (UTC). */
export function csvFileName(title, now = new Date()) {
  const slug = String(title ?? "").toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 60) || "table";
  const p = (n) => String(n).padStart(2, "0");
  const stamp =
    now.getUTCFullYear() + p(now.getUTCMonth() + 1) + p(now.getUTCDate()) + "-" + p(now.getUTCHours()) + p(now.getUTCMinutes()) + p(now.getUTCSeconds());
  return slug + "-" + stamp + ".csv";
}

export const COPY_UNAVAILABLE = "Copy isn't available here: the browser only allows it on a secure (HTTPS or localhost) page.";

/** Puts text on the clipboard. Resolves to { ok, message }; never throws, so the caller can always say what happened. */
export async function copyText(text) {
  const nav = typeof navigator !== "undefined" ? navigator : null;
  const secure = typeof window === "undefined" || window.isSecureContext !== false;
  if (!nav || !nav.clipboard || typeof nav.clipboard.writeText !== "function" || !secure) {
    return { ok: false, message: COPY_UNAVAILABLE };
  }
  try {
    await nav.clipboard.writeText(text);
    return { ok: true, message: "Copied." };
  } catch (e) {
    return { ok: false, message: "Copy failed: " + (e && e.message ? e.message : "the browser refused.") };
  }
}

const REVOKE_AFTER_MS = 10000;

/** Hands text to the browser as a file download under the given MIME type. */
export function downloadText(fileName, parts, type) {
  const blob = new Blob(parts, { type });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = fileName;
  a.style.display = "none";
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  /* Some browsers cancel the download if the URL is revoked before it has started. */
  setTimeout(() => URL.revokeObjectURL(url), REVOKE_AFTER_MS);
}

/** Hands CSV text to the browser as a file download (UTF-8 with a byte-order mark so a spreadsheet reads unicode). */
export function downloadCsv(fileName, csv) {
  downloadText(fileName, ["\uFEFF", csv], "text/csv;charset=utf-8");
}
