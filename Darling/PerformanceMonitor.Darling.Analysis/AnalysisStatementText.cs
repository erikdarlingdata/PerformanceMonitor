/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// #5320: statement text in an analysis answer is judged WHOLE, then cut. The readers used to cut it in SQL
/// (<c>LEFT(query_text, 500)</c>) and judge only what was left, so a value that sat early in a batch with the
/// text that names it past the cut (a URI's <c>user:secret@</c> trigger is its closing at-sign) read clean.
/// A reader now returns the whole text and calls <see cref="Preview"/> on it.
/// </summary>
internal static class AnalysisStatementText
{
    /// <summary>
    /// <paramref name="raw"/> judged by <see cref="SensitiveStatements.Text"/>, then cut to at most
    /// <paramref name="maxCodePoints"/> characters. PostgreSQL's <c>LEFT(text, n)</c> counts code points, so the
    /// cut does too, and a kept statement reads exactly as the SQL cut left it. A statement the filter names is its
    /// placeholder, so no cut text can hold half of one. Null stays null.
    /// </summary>
    public static string? Preview(string? raw, int maxCodePoints)
    {
        var judged = SensitiveStatements.Text(raw);
        return judged == null ? null : CutCodePoints(judged, maxCodePoints);
    }

    /// <summary>The first <paramref name="maxCodePoints"/> code points of <paramref name="text"/>, never splitting a
    /// surrogate pair.</summary>
    internal static string CutCodePoints(string text, int maxCodePoints)
    {
        if (text.Length <= maxCodePoints) return text;
        var i = 0;
        for (var n = 0; n < maxCodePoints && i < text.Length; n++)
            i += char.IsHighSurrogate(text[i]) && i + 1 < text.Length && char.IsLowSurrogate(text[i + 1]) ? 2 : 1;
        return i >= text.Length ? text : text[..i];
    }
}
