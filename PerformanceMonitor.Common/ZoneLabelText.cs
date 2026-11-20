/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Common;

/// <summary>
/// Text rules for the Server Time offset label ("(UTC-05:00)") that a card's Last Collect line carries. Lite's Overview card and the
/// Darling viewer's card both wrap that line, so both use this one copy (#9094, #3684).
/// </summary>
public static class ZoneLabelText
{
    /// <summary>
    /// #9094: the Last Collect row wraps (a dated stamp with its state word is wider than the room beside the label), and a narrow
    /// card broke the Server Time offset in two, "(UTC" on one line and "-05:00)" on the next. A word joiner (U+2060) between "UTC"
    /// and the sign removes that break point, so the offset moves to the next line whole. A card's own display text only: the time
    /// formatters keep their plain text.
    /// </summary>
    public static string KeepWhole(string text) =>
        text.Replace("UTC+", "UTC\u2060+", StringComparison.Ordinal).Replace("UTC-", "UTC\u2060-", StringComparison.Ordinal);
}
