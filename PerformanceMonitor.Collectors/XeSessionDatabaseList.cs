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

namespace PerformanceMonitor.Collectors;

/// <summary>
/// #7639: the per-server list of the databases that get Extended Events sessions on Azure SQL Database, where each capture
/// (deadlock, blocked process, client timeout, long query completions) keeps one database-scoped session per database and
/// Azure caps them per pool. Lite and Darling both narrow their create lists here, so the two apps choose the same databases.
/// An empty list means every database, which is how it worked before the setting. The server's excluded databases still apply
/// first and win: a database on both lists is already gone from the list this narrows. A server-scoped session on a box SQL
/// Server covers every database and is never filtered by this.
/// </summary>
public static class XeSessionDatabaseList
{
    /// <summary>
    /// The run note for a list that is set but names no database the server has: nothing is attempted, so nothing is refused, and
    /// without this the capture's runs record SUCCESS with no rows and no explanation. What gets created does not change.
    /// </summary>
    public const string MatchesNoDatabaseNote =
        "The XE session database list matches no database on this server, so no XE session was created. Edit the list, or clear it to use every database.";

    /// <summary>
    /// Whether <paramref name="included"/> names at least one database and <see cref="Apply"/> left none of the
    /// <paramref name="before"/> databases. A list that is empty, or a server with nothing to narrow, is not this case.
    /// </summary>
    public static bool MatchesNone(IReadOnlyCollection<string> before, IEnumerable<string>? included, IReadOnlyCollection<string> after) =>
        before.Count > 0
        && after.Count == 0
        && (included ?? Array.Empty<string>()).Any(name => !string.IsNullOrWhiteSpace(name));

    /// <summary>
    /// <paramref name="databases"/> narrowed to the names on <paramref name="included"/> (case-insensitive, trimmed), in the
    /// order given. Returns every database when <paramref name="included"/> is null, empty or only blanks. Only a create list
    /// goes through here: a session this install made in a database left off the list is left alone, never dropped. The ownerless legacy long-query session is not governed by the list.
    /// </summary>
    public static IReadOnlyList<string> Apply(IEnumerable<string> databases, IEnumerable<string>? included)
    {
        var wanted = (included ?? Array.Empty<string>())
            .Where(name => !string.IsNullOrWhiteSpace(name))
            .Select(name => name.Trim())
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        return wanted.Count == 0
            ? databases.ToList()
            : databases.Where(database => wanted.Contains(database.Trim())).ToList();
    }
}
