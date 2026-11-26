/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text;

namespace PerformanceMonitor.Common;

/// <summary>
/// The one rule every path that takes a server's display name, host, database or login applies to the text (#9787): none of
/// the four holds a control character. A line feed, a carriage return, a tab, DEL, any other C0 or C1 control character,
/// U+2028 or U+2029, or a Unicode format character (a right-to-left override, a zero-width mark) would start what reads
/// as a second entry in a log, a drift warning or a printed command. The web add and edit routes, the MCP
/// <c>add_servers</c> and <c>edit_server</c> tools, the command-line add and the Darling Viewer's save all call
/// <see cref="Refusal"/>, so every surface refuses alike.
///
/// <para>A row saved before the rule existed can still hold one. <see cref="Escape"/> writes such a value into a log line
/// or a printed line as visible <c>\uXXXX</c> text instead of the character.</para>
/// </summary>
public static class ServerFieldText
{
    /// <summary>True for a character the rule refuses: a C0 or C1 control character (tab and DEL included), U+2028,
    /// U+2029, or a Unicode format character.</summary>
    public static bool IsControl(char c) =>
        char.IsControl(c)
        || c == '\u2028'
        || c == '\u2029'
        || CharUnicodeInfo.GetUnicodeCategory(c) == UnicodeCategory.Format;

    /// <summary>True when <paramref name="value"/> holds a character <see cref="IsControl"/> refuses.</summary>
    public static bool HasControlCharacter(string? value)
    {
        foreach (var c in value ?? string.Empty)
        {
            if (IsControl(c))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Null when <paramref name="value"/> may be saved as <paramref name="field"/>, otherwise the one sentence to refuse with.
    /// It names the field and the character class and never repeats the value. Null or blank text is not this rule's business
    /// (callers refuse a missing value themselves), so it returns null.
    /// </summary>
    public static string? Refusal(string field, string? value) =>
        HasControlCharacter(value)
            ? $"{field} holds a control character (a line break, a tab, DEL, another control or format character, or a line or paragraph separator), which is not accepted."
            : null;

    /// <summary>The first refusal over a whole definition's four fields, or null when none holds a control character. The host is
    /// asked first because a definition that names no display name takes its host as the name, and the sentence should
    /// name the field that was typed.</summary>
    public static string? FirstRefusal(string? displayName, string? host, string? database, string? username) =>
        Refusal("host", host)
        ?? Refusal("display_name", displayName)
        ?? Refusal("database", database)
        ?? Refusal("username", username);

    /// <summary>
    /// <paramref name="value"/> as text that is safe on one log or printed line: every character <see cref="IsControl"/>
    /// refuses is written as <c>\uXXXX</c>, and every other character, a lone surrogate included, is left as it is.
    /// Null is the empty string.
    /// </summary>
    public static string Escape(string? value)
    {
        if (!HasControlCharacter(value))
        {
            return value ?? string.Empty;
        }

        var builder = new StringBuilder(value!.Length + 8);
        foreach (var c in value)
        {
            if (IsControl(c))
            {
                builder.Append("\\u").Append(((int)c).ToString("X4", CultureInfo.InvariantCulture));
            }
            else
            {
                builder.Append(c);
            }
        }

        return builder.ToString();
    }
}
