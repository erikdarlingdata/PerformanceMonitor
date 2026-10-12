/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Common;

/// <summary>
/// The one spelling of the note the Locking &amp; Contention views and <c>get_object_locking</c> carry when a
/// database on the server has optimized locking on. Writers on such a database wait on transaction-ID locks, and
/// the per-index row and page lock counters those views read do not count them, so a zero there does not mean
/// nothing blocked. Shared by Lite and Darling so both apps, both WPF views and the web say the same words.
/// </summary>
public static class OptimizedLockingNote
{
    /// <summary>The note text. Identical in every surface.</summary>
    public const string Text =
        "On a database with optimized locking, writers wait on transaction-ID locks, which these per-index "
        + "counters don't count, so zero here doesn't mean no blocking. See the blocking and wait views.";

    /// <summary>
    /// The note when ANY database's newest stored <c>is_optimized_locking_on</c> is true; null otherwise. A
    /// null flag (unknown) and a false flag both show no note.
    /// </summary>
    public static string? For(IEnumerable<bool?> newestFlagPerDatabase)
    {
        foreach (var flag in newestFlagPerDatabase)
        {
            if (flag == true) return Text;
        }
        return null;
    }
}
