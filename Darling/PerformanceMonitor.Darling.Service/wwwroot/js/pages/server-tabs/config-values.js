/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* #7416 item 3: the From / To cells of the configuration change grids.
 *
 * A setting whose new value only gained a trailing space (log_line_prefix) drew two cells that read the same, so the row looked like
 * a change that changed nothing. When the two values differ ONLY in whitespace, both cells show their whitespace: a space as a
 * middle dot, a tab as an arrow, a line break as a return mark, any other white-space character as its code point, with the value
 * in quotes so an end can be seen. Every other value draws as stored. The desktop viewers' ConfigValueText.VisibleWhitespace is
 * the same rule. This module imports nothing, so a harness can run it as shipped. */

const MARKS = { " ": "·", "\t": "→", "\n": "↵", "\r": "␍" };

/** True when the two strings are not equal but are equal once every white-space character is removed. */
export function differOnlyInWhitespace(a, b) {
  if (typeof a !== "string" || typeof b !== "string" || a === b) return false;
  return a.replace(/\s/g, "") === b.replace(/\s/g, "");
}

/** The text with each white-space character made visible, inside quotes. */
export function visibleWhitespace(text) {
  const body = Array.from(text, (ch) => {
    if (MARKS[ch]) return MARKS[ch];
    if (/\s/.test(ch)) return "<U+" + ch.codePointAt(0).toString(16).toUpperCase().padStart(4, "0") + ">";
    return ch;
  }).join("");
  return "“" + body + "”";
}

/** What one From or To cell shows: the value as stored, or its visible-whitespace form when the pair differs only in whitespace.
 *  Null for an absent or empty value, so the grid draws its usual dash. */
export function configValueCell(row, key, otherKey) {
  const own = row[key];
  if (own == null) return null;
  if (differOnlyInWhitespace(own, row[otherKey])) return visibleWhitespace(own);
  return own === "" ? null : String(own);
}
