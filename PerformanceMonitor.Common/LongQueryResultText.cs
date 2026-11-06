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
/// The Result cell of the long query completions grids (#7699 item 1). The XE <c>result</c> field is a map: 0 OK, 1 Error,
/// 2 Abort, 3 Skipped. The collector now stores the map's own text, but rows stored before that hold the number, so every
/// read passes the stored value through <see cref="Word"/> and both kinds print as words. Any other value prints as stored.
/// </summary>
public static class LongQueryResultText
{
    /// <summary>The map word for a stored number, the value itself for a word or an unknown number, null for null.</summary>
    public static string? Word(string? stored)
    {
        if (stored is null)
            return null;
        return stored.Trim() switch
        {
            "0" => "OK",
            "1" => "Error",
            "2" => "Abort",
            "3" => "Skipped",
            _ => stored,
        };
    }
}
