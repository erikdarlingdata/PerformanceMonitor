/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// The text form of the Settings windows' alert filter lists (#6112): one entry per line. A database name may
/// hold a comma (<c>Sales,2024</c>), so a comma cannot be the separator of the Excluded databases box; a line
/// break cannot occur inside a name. Both products' windows load and save through here so the two read one
/// definition.
/// </summary>
public static class SettingsListText
{
    /// <summary>The list as box text: one entry per line.</summary>
    public static string Join(IEnumerable<string>? entries)
    {
        return entries is null ? "" : string.Join(Environment.NewLine, entries);
    }

    /// <summary>Box text as a list for names that may hold a comma: split on line breaks only (<c>\r\n</c>,
    /// <c>\n</c> or a bare <c>\r</c>), each line trimmed, blank lines dropped. A comma stays inside its name.</summary>
    public static List<string> SplitLines(string? text)
    {
        return (text ?? "")
            .Split(new[] { "\r\n", "\n", "\r" }, StringSplitOptions.None)
            .Select(s => s.Trim())
            .Where(s => s.Length > 0)
            .ToList();
    }

    /// <summary>Box text as a list for entries that never hold a comma (excluded volumes): split on line breaks
    /// and on commas, so a comma-separated paste from an older build's box still works.</summary>
    public static List<string> SplitLinesAndCommas(string? text)
    {
        return (text ?? "")
            .Split(new[] { "\r\n", "\n", "\r", "," }, StringSplitOptions.None)
            .Select(s => s.Trim())
            .Where(s => s.Length > 0)
            .ToList();
    }
}
