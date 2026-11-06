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

namespace PerformanceMonitor.Common;

/// <summary>
/// #7706: the one sentence for a database this server holds only as an availability group secondary copy and that has no PVS row in
/// the snapshot (the DMV reports nothing for it here), so the Version Store grid does not look as if the database were forgotten.
/// One copy for every surface in both products: the MCP tool (<c>secondary_replica_note</c> on <c>get_pvs_stats</c>, which the web page
/// shows) and the desktop grid strip, in Darling and in Lite. It lives beside <see cref="AgReplicaScope"/> because the set of
/// secondary databases is that class's decision.
/// </summary>
public static class PvsSecondaryNote
{
    /// <summary>The JSON key the note travels under on <c>get_pvs_stats</c>, on its rows reply and on its empty reply.</summary>
    public const string Key = "secondary_replica_note";

    /// <summary>
    /// The instant the replica roles are judged at: the time of the snapshot the note explains, so a snapshot that is hours old is
    /// judged against the roles it was taken under and not today's. <paramref name="snapshotUtc"/> is the newest row's collection time,
    /// or null when no row came back (an empty reply), which is judged now. Every surface that builds the note calls this one rule.
    /// </summary>
    public static DateTime ScopeTimeUtc(DateTime? snapshotUtc) =>
        snapshotUtc is { } snapshot ? DateTime.SpecifyKind(snapshot, DateTimeKind.Utc) : DateTime.UtcNow;

    /// <summary>
    /// The sentence for the secondary databases that have no row in <paramref name="rowDatabases"/> and are among the databases the
    /// caller chose. <paramref name="chosenNames"/> null or empty means every database; <paramref name="excludedNames"/> are left out
    /// whatever was chosen. A name is matched exactly, as <see cref="AgReplicaScope"/> does. Null when none.
    /// </summary>
    public static string? WithoutRow(
        IEnumerable<string> secondaryDatabases,
        IEnumerable<string> rowDatabases,
        IReadOnlyList<string>? chosenNames = null,
        IReadOnlyList<string>? excludedNames = null)
    {
        var withRow = new HashSet<string>(rowDatabases, StringComparer.Ordinal);
        var count = secondaryDatabases.Count(d => !withRow.Contains(d)
            && (chosenNames is not { Count: > 0 } || chosenNames.Contains(d, StringComparer.Ordinal))
            && (excludedNames is null || !excludedNames.Contains(d, StringComparer.Ordinal)));
        return count switch
        {
            0 => null,
            1 => "1 database has no row here: this server holds a secondary copy of it in an availability group. Check the primary replica for its persistent version store.",
            _ => string.Create(System.Globalization.CultureInfo.InvariantCulture,
                $"{count} databases have no row here: this server holds a secondary copy of them in an availability group. Check the primary replica for their persistent version store."),
        };
    }
}
