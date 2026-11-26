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
    /// U+2029, or a Unicode format character. A surrogate half is not refused here, because <see cref="IsControl(Rune)"/>
    /// judges the pair it belongs to.</summary>
    public static bool IsControl(char c) =>
        char.IsControl(c)
        || c == '\u2028'
        || c == '\u2029'
        || CharUnicodeInfo.GetUnicodeCategory(c) == UnicodeCategory.Format;

    /// <summary>The same rule over a whole code point, so a format character outside the Basic Multilingual Plane (the
    /// invisible tag characters U+E0000 to U+E007F among them) is refused like one inside it.</summary>
    public static bool IsControl(Rune rune) =>
        Rune.IsControl(rune)
        || rune.Value is 0x2028 or 0x2029
        || Rune.GetUnicodeCategory(rune) == UnicodeCategory.Format;

    /// <summary>True when <paramref name="value"/> holds a character <see cref="IsControl(Rune)"/> refuses.</summary>
    public static bool HasControlCharacter(string? value)
    {
        var text = value ?? string.Empty;
        for (var i = 0; i < text.Length; i += Width(text, i))
        {
            if (Refused(text, i))
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
    /// <paramref name="value"/> as text that is safe on one log or printed line: every character <see cref="IsControl(Rune)"/>
    /// refuses is written as <c>\uXXXX</c>, or <c>\UXXXXXXXX</c> outside the Basic Multilingual Plane, and every other
    /// character, a lone surrogate included, is left as it is. Null is the empty string.
    /// </summary>
    public static string Escape(string? value)
    {
        if (!HasControlCharacter(value))
        {
            return value ?? string.Empty;
        }

        var text = value!;
        var builder = new StringBuilder(text.Length + 8);
        for (var i = 0; i < text.Length;)
        {
            var width = Width(text, i);
            if (!Refused(text, i))
            {
                builder.Append(text, i, width);
            }
            else if (width == 1)
            {
                builder.Append("\\u").Append(((int)text[i]).ToString("X4", CultureInfo.InvariantCulture));
            }
            else
            {
                builder.Append("\\U").Append(char.ConvertToUtf32(text[i], text[i + 1]).ToString("X8", CultureInfo.InvariantCulture));
            }

            i += width;
        }

        return builder.ToString();
    }

    /* Two for a surrogate pair, one for any other char, a lone surrogate included. */
    private static int Width(string text, int index) =>
        index + 1 < text.Length && char.IsSurrogatePair(text[index], text[index + 1]) ? 2 : 1;

    private static bool Refused(string text, int index) =>
        Width(text, index) == 2
            ? IsControl(new Rune(text[index], text[index + 1]))
            : IsControl(text[index]);
}
