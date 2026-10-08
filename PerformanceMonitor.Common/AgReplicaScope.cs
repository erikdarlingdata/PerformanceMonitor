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

/// <summary>One database-on-one-replica row from the <c>ag_database_replica_states</c> snapshot, reduced to the
/// columns the replica scope judges (#5558). Every column but the names is nullable on purpose: a NULL
/// <c>IsLocal</c> (a row from before the column, or WSFC quorum loss) is "cannot tell", never "secondary".</summary>
public readonly record struct AgDatabaseMembership(string AgName, string DatabaseName, bool? IsLocal);

/// <summary>
/// Which databases this node holds only as a SECONDARY copy in an Availability Group (#5558). A secondary's
/// database-level settings are the primary's (they replicate), so every node would report the same finding for
/// the same database; the primary reports it once and the Recommendations skip it everywhere else.
///
/// <para>Read from the monitoring store's own <c>ag_replica_states</c> / <c>ag_database_replica_states</c>
/// snapshots, not from a live <c>sys.fn_hadr_is_primary_replica</c> call: it costs no new query on the monitored
/// server, it works for an AsOf pass (the role at that time, not now), and it fails open where the AG views do
/// not exist (Azure SQL Database). See
/// https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-hadr-database-replica-states-transact-sql
/// and
/// https://learn.microsoft.com/en-us/sql/relational-databases/system-functions/sys-fn-hadr-is-primary-replica-transact-sql
/// for the engine's own definition of primary and secondary.</para>
///
/// <para><b>Fail open, always.</b> A database is skipped only when BOTH snapshots exist and are fresh, the database
/// has a LOCAL row in the database snapshot, and the LOCAL replica's role for that database's group is explicitly
/// SECONDARY. No rows, stale rows, a NULL role or <c>is_local</c>, RESOLVING, a standalone database and an engine
/// without the AG views all skip nothing. The role is per group: two groups on one instance can have different
/// primaries, so <c>server_properties.ag_replica_role</c> (one value per instance) is never used.</para>
/// </summary>
public static class AgReplicaScope
{
    /// <summary>How old a snapshot may be, measured against the pass's window end, before it is "stale" and
    /// vouches for nothing. The AG collectors run every minute by default; an hour tolerates a slower custom
    /// cadence without letting a stopped collector keep a days-old role in force.</summary>
    public static readonly TimeSpan SnapshotFreshness = TimeSpan.FromHours(1);

    /// <summary>The role string the engine reports for a secondary replica.</summary>
    public const string SecondaryRole = "SECONDARY";

    /// <summary>True when <paramref name="snapshotUtc"/> exists, is no later than the window end, and is within
    /// <see cref="SnapshotFreshness"/> of it.</summary>
    public static bool IsFresh(DateTime? snapshotUtc, DateTime windowEndUtc) =>
        snapshotUtc is { } at && at <= windowEndUtc && windowEndUtc - at <= SnapshotFreshness;

    /// <summary>The secondary databases from two snapshots read as of <paramref name="windowEndUtc"/>, applying
    /// the freshness rule. Empty (never null) whenever anything is missing or stale.</summary>
    public static IReadOnlySet<string> SecondaryDatabases(
        IReadOnlyList<AgReplicaReading>? replicas, DateTime? replicaSnapshotUtc,
        IReadOnlyList<AgDatabaseMembership>? databases, DateTime? databaseSnapshotUtc,
        DateTime windowEndUtc)
    {
        if (!IsFresh(replicaSnapshotUtc, windowEndUtc) || !IsFresh(databaseSnapshotUtc, windowEndUtc))
            return Empty();
        return SecondaryDatabases(replicas, databases);
    }

    /// <summary>The secondary databases from two snapshots already judged fresh. A database is in the set only
    /// when its group has exactly one distinct local role and that role is SECONDARY.</summary>
    public static IReadOnlySet<string> SecondaryDatabases(
        IReadOnlyList<AgReplicaReading>? replicas, IReadOnlyList<AgDatabaseMembership>? databases)
    {
        if (replicas is not { Count: > 0 } || databases is not { Count: > 0 }) return Empty();

        /* Local role per group. Two local rows for one group that disagree (a torn snapshot) are "cannot tell". */
        var roleByGroup = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        foreach (var replica in replicas)
        {
            if (replica.IsLocal != true || string.IsNullOrWhiteSpace(replica.AgName)) continue;
            var role = string.IsNullOrWhiteSpace(replica.RoleDesc) ? null : replica.RoleDesc.Trim().ToUpperInvariant();
            if (roleByGroup.TryGetValue(replica.AgName, out var seen))
            {
                if (!string.Equals(seen, role, StringComparison.Ordinal)) roleByGroup[replica.AgName] = null;
            }
            else
            {
                roleByGroup[replica.AgName] = role;
            }
        }

        var secondary = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var notSecondary = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var database in databases)
        {
            if (database.IsLocal != true || string.IsNullOrWhiteSpace(database.DatabaseName)
                || string.IsNullOrWhiteSpace(database.AgName)) continue;
            var isSecondary = roleByGroup.TryGetValue(database.AgName, out var localRole)
                && string.Equals(localRole, SecondaryRole, StringComparison.Ordinal);
            if (isSecondary) secondary.Add(database.DatabaseName);
            else notSecondary.Add(database.DatabaseName);
        }

        /* A name that also reads as not-secondary (two groups claiming one name, a torn snapshot) is not skipped. */
        secondary.ExceptWith(notSecondary);
        return secondary;
    }

    /// <summary>The one-sentence note shown wherever databases were skipped, or null for none. Plain English;
    /// the same sentence on every surface.</summary>
    public static string? SkippedNote(int count) => count switch
    {
        <= 0 => null,
        1 => "1 database skipped: this server holds a secondary copy of it in an availability group. Check the primary replica for its findings.",
        _ => string.Create(CultureInfo.InvariantCulture,
            $"{count} databases skipped: this server holds a secondary copy of them in an availability group. Check the primary replica for their findings."),
    };

    /// <summary>The note for a set, or null when it is null or empty.</summary>
    public static string? SkippedNote(IReadOnlyCollection<string>? secondaryDatabases) =>
        secondaryDatabases is null ? null : SkippedNote(secondaryDatabases.Count);

    /// <summary>True when <paramref name="databaseName"/> is in the skipped set (case-insensitive, like the engine's
    /// database names). A null or empty set, or a null/blank name, is false: fail open.</summary>
    public static bool IsSkipped(IReadOnlyCollection<string>? secondaryDatabases, string? databaseName)
    {
        if (secondaryDatabases is not { Count: > 0 } || string.IsNullOrWhiteSpace(databaseName)) return false;
        foreach (var name in secondaryDatabases)
            if (string.Equals(name, databaseName, StringComparison.OrdinalIgnoreCase)) return true;
        return false;
    }

    /// <summary>The names that are not a secondary copy on this node, in their original order. The FinOps
    /// Recommendations list filters its per-database findings with this (#5558). With nothing to skip the input
    /// comes back unchanged.</summary>
    public static List<string> WithoutSecondaries(IEnumerable<string> databaseNames, IReadOnlyCollection<string>? secondaryDatabases) =>
        databaseNames.Where(name => !IsSkipped(secondaryDatabases, name)).ToList();

    private static HashSet<string> Empty() => new(StringComparer.OrdinalIgnoreCase);
}
