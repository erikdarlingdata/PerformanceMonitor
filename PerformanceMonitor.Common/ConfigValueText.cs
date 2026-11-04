/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text;

namespace PerformanceMonitor.Common;

/// <summary>
/// The From / To (Old / New) cells of the configuration change grids (#7416 item 3). A setting whose new value only gained a
/// trailing space (<c>log_line_prefix</c>) drew two cells that read the same, so the row looked like a change that changed
/// nothing. When the two values differ ONLY in whitespace, both cells show their whitespace: a space as a middle dot, a tab as an
/// arrow, a line break as a return mark, any other white-space character as its code point, with the value in quotes so an end
/// can be seen, and the cell says "(white space only)" (#7483). Every other value draws as stored. The web grids' <c>config-values.js</c> is the same rule.
/// </summary>
public static class ConfigValueText
{
    /// <summary>True when the two values are not equal but are equal once every white-space character is removed.</summary>
    public static bool DifferOnlyInWhitespace(string? a, string? b)
    {
        if (a is null || b is null || string.Equals(a, b, StringComparison.Ordinal))
            return false;

        return string.Equals(StripWhitespace(a), StripWhitespace(b), StringComparison.Ordinal);
    }

    /// <summary>The text with each white-space character made visible, inside quotes.</summary>
    public static string VisibleWhitespace(string text)
    {
        var builder = new StringBuilder(text.Length + 2);
        builder.Append('“');
        foreach (var rune in text.EnumerateRunes())
        {
            switch (rune.Value)
            {
                case ' ': builder.Append('·'); break;
                case '\t': builder.Append('→'); break;
                case '\n': builder.Append('↵'); break;
                case '\r': builder.Append('␍'); break;
                default:
                    if (Rune.IsWhiteSpace(rune))
                        builder.Append("<U+").Append(rune.Value.ToString("X4", CultureInfo.InvariantCulture)).Append('>');
                    else
                        builder.Append(rune.ToString());
                    break;
            }
        }

        return builder.Append('”').ToString();
    }

    /// <summary>What the cell for <paramref name="own"/> shows beside <paramref name="other"/>: the value as stored, or its
    /// visible-whitespace form when the pair differs only in whitespace. Null stays null.</summary>
    public static string? Shown(string? own, string? other) =>
        own is not null && DifferOnlyInWhitespace(own, other) ? VisibleWhitespace(own) : own;

    private static string StripWhitespace(string text) => new(text.Where(c => !char.IsWhiteSpace(c)).ToArray());
}
