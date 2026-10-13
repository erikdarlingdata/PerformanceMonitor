/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace PerformanceMonitor.Common;

/// <summary>
/// #5633 review (finding D): what a ranking read tells its reader when its database narrowing left databases out. With no
/// <c>database_name</c>, <c>get_top_procedures_by_cpu</c> leaves out the databases the server dropped since an earlier
/// <c>database_states</c> snapshot (#5633 item 4). That is right for the ranking, but an empty page then must not read as
/// "the collector recorded nothing": the omitted databases may be the only place a procedure ran. The names and the one note
/// below travel in the envelope of every answer, empty or not, in the same words on both products.
/// </summary>
public static class DatabasesLeftOut
{
    /// <summary>The most names the envelope lists; the note carries the full count, so a server with hundreds of dropped test
    /// databases does not bloat every page.</summary>
    public const int MaxNamed = 20;

    /// <summary>The envelope key of the omitted names.</summary>
    public const string NamesKey = "databases_left_out";

    /// <summary>The envelope key of <see cref="Note"/>.</summary>
    public const string NoteKey = "databases_left_out_note";

    /// <summary>The omitted names, ordinal-sorted and distinct, or null when none were left out. Not capped: use
    /// <see cref="Named"/> for the envelope.</summary>
    public static IReadOnlyList<string>? Distinct(IEnumerable<string?>? names)
    {
        var sorted = (names ?? Array.Empty<string?>())
            .Where(name => !string.IsNullOrWhiteSpace(name))
            .Select(name => name!)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToList();
        return sorted.Count == 0 ? null : sorted;
    }

    /// <summary>The first <see cref="MaxNamed"/> of <paramref name="all"/> for the envelope, or null when none.</summary>
    public static IReadOnlyList<string>? Named(IReadOnlyList<string>? all)
        => all is null || all.Count == 0 ? null : all.Count <= MaxNamed ? all : all.Take(MaxNamed).ToList();

    /// <summary>The one note beside the names, or null when none were left out.</summary>
    public static string? Note(IReadOnlyList<string>? all)
    {
        if (all is null || all.Count == 0)
        {
            return null;
        }

        var more = all.Count > MaxNamed
            ? string.Create(CultureInfo.InvariantCulture, $" ({all.Count} in all; the first {MaxNamed} are listed)")
            : "";
        return "These databases were left out of this read because the server's newest database snapshot no longer lists them "
            + "(dropped or detached since an earlier snapshot did)" + more + ". Name one in database_name to read its history.";
    }
}
