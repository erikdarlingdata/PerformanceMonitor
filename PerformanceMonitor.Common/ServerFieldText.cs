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
/// The one rule every path that takes a server's display name, host, database or login applies to the text (#9787). None of
/// the four holds a character that would start what reads as a second entry in a log, a drift warning or a printed command,
/// or change what a reader sees: a C0 or C1 control character (line feed, carriage return, tab and DEL included), U+2028 or
/// U+2029, a bidirectional embedding, override or isolate (U+202A to U+202E, U+2066 to U+2069), or an invisible tag
/// character (U+E0000 to U+E007F). The rest of the Unicode format category is accepted: the zero-width joiner and
/// non-joiner are part of ordinary words in Persian and several Indic scripts, and the left-to-right and right-to-left marks
/// are part of Hebrew and Arabic text, so a real database or login can hold one. The web add and edit routes, the MCP
/// <c>add_servers</c> and <c>edit_server</c> tools, the command-line add, the Darling Viewer's save and Lite's add paths all
/// call <see cref="Refusal"/>, so every surface refuses alike.
///
/// <para>A row saved before the rule existed can still hold one. <see cref="Escape"/> writes a value into a log line or a
/// printed line with every control and format character as visible <c>\uXXXX</c> text, so an accepted joiner or mark is
/// visible there too.</para>
/// </summary>
public static class ServerFieldText
{
    /// <summary>True for a character <see cref="Escape"/> writes visibly: a C0 or C1 control character (tab and DEL
    /// included), U+2028, U+2029, or any Unicode format character. A surrogate half is not one here, because
    /// <see cref="IsControl(Rune)"/> judges the pair it belongs to.</summary>
    public static bool IsControl(char c) =>
        char.IsControl(c)
        || c == '\u2028'
        || c == '\u2029'
        || CharUnicodeInfo.GetUnicodeCategory(c) == UnicodeCategory.Format;

    /// <summary>The same set over a whole code point, so a format character outside the Basic Multilingual Plane is
    /// escaped like one inside it.</summary>
    public static bool IsControl(Rune rune) =>
        Rune.IsControl(rune)
        || rune.Value is 0x2028 or 0x2029
        || Rune.GetUnicodeCategory(rune) == UnicodeCategory.Format;

    /// <summary>True for a code point the save rule refuses: a C0 or C1 control character, U+2028, U+2029, a
    /// bidirectional embedding, override or isolate, or a tag character.</summary>
    public static bool IsRefused(Rune rune) =>
        Rune.IsControl(rune)
        || rune.Value is 0x2028 or 0x2029
        || rune.Value is >= 0x202A and <= 0x202E
        || rune.Value is >= 0x2066 and <= 0x2069
        || rune.Value is >= 0xE0000 and <= 0xE007F;

    /// <summary>True when <paramref name="value"/> holds a code point <see cref="IsRefused"/> refuses.</summary>
    public static bool HasRefusedCharacter(string? value) => Any(value, refused: true);

    /// <summary>
    /// Null when <paramref name="value"/> may be saved as <paramref name="field"/>, otherwise the one sentence to refuse with.
    /// It names the field and the character class and never repeats the value. Null or blank text is not this rule's business
    /// (callers refuse a missing value themselves), so it returns null.
    /// </summary>
    public static string? Refusal(string field, string? value) =>
        HasRefusedCharacter(value)
            ? $"{field} holds a control character (a line break, a tab, DEL, another control character, a line or paragraph separator, a bidirectional embedding, override or isolate, or an invisible tag character), which is not accepted."
            : null;

    /// <summary>The first refusal over a whole definition's four fields, or null when none holds a refused character. The host
    /// is asked first because a definition that names no display name takes its host as the name, and the sentence should
    /// name the field that was typed.</summary>
    public static string? FirstRefusal(string? displayName, string? host, string? database, string? username) =>
        Refusal("host", host)
        ?? Refusal("display_name", displayName)
        ?? Refusal("database", database)
        ?? Refusal("username", username);

    /// <summary>
    /// <paramref name="value"/> as text that is safe on one log or printed line: every character <see cref="IsControl(Rune)"/>
    /// names is written as <c>\uXXXX</c>, or <c>\UXXXXXXXX</c> outside the Basic Multilingual Plane, and every other
    /// character, a lone surrogate included, is left as it is. Null is the empty string.
    /// </summary>
    public static string Escape(string? value)
    {
        if (!Any(value, refused: false))
        {
            return value ?? string.Empty;
        }

        var text = value!;
        var builder = new StringBuilder(text.Length + 8);
        for (var i = 0; i < text.Length;)
        {
            var width = Width(text, i);
            if (!Matches(text, i, refused: false))
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

    private static bool Any(string? value, bool refused)
    {
        var text = value ?? string.Empty;
        for (var i = 0; i < text.Length; i += Width(text, i))
        {
            if (Matches(text, i, refused))
            {
                return true;
            }
        }

        return false;
    }

    /* Two for a surrogate pair, one for any other char, a lone surrogate included. */
    private static int Width(string text, int index) =>
        index + 1 < text.Length && char.IsSurrogatePair(text[index], text[index + 1]) ? 2 : 1;

    /* A lone surrogate is neither refused nor escaped: it is no code point, and Rune cannot hold it. */
    private static bool Matches(string text, int index, bool refused)
    {
        Rune rune;
        if (Width(text, index) == 2)
        {
            rune = new Rune(text[index], text[index + 1]);
        }
        else if (!Rune.TryCreate(text[index], out rune))
        {
            return false;
        }

        return refused ? IsRefused(rune) : IsControl(rune);
    }
}
