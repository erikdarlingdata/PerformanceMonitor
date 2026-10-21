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
/// Per-server tracker of which databases have a capturing secondary (#6279), learned from the rows the collector
/// already reads: a database has one when a row it read in the last 24 hours belongs to a replica group other
/// than the primary's, that is, <see cref="QueryStoreCollector.Row.ReplicaRole"/> is set and is not the primary's
/// name. No statement of its own runs against the monitored server. (Measured on the SQL Server 2025 CU9
/// fixture: <c>sys.query_store_replicas</c> is a static four-row roster whether or not a secondary captures, so
/// it cannot detect one.) The flag lapses after <see cref="Lapse"/> without such a row; once set, the
/// look-back read keeps it alive, because it sees the late rows. After a restart nothing is known until the host
/// seeds it (<see cref="Seed"/>, from one store query) or the first read sees such a row. One instance per
/// server; safe for one runner at a time.
/// </summary>
public sealed class QueryStoreCapturingSecondaryTracker
{
    /// <summary>How long a database keeps the flag without a non-primary row.</summary>
    public static readonly TimeSpan Lapse = TimeSpan.FromHours(24);

    /// <summary>The primary's name in <c>sys.query_store_replicas.replica_name</c> and so in <c>replica_role</c>.</summary>
    public const string PrimaryRoleName = "Primary";

    private readonly object _gate = new();
    private readonly Dictionary<string, DateTime> _lastSeen = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Whether a role names a group other than the primary's. NULL (not attributed) does not.</summary>
    public static bool IsNonPrimaryRole(string? replicaRole) =>
        !string.IsNullOrEmpty(replicaRole)
        && !string.Equals(replicaRole, PrimaryRoleName, StringComparison.OrdinalIgnoreCase);

    /// <summary>Records a read's rows (before the ship guard filters them): any non-primary row stamps the database.</summary>
    public void Observe(string database, IEnumerable<QueryStoreCollector.Row> rows, DateTime now)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentNullException.ThrowIfNull(rows);

        if (!rows.Any(r => IsNonPrimaryRole(r.ReplicaRole)))
        {
            return;
        }

        lock (_gate)
        {
            _lastSeen[database] = now;
        }
    }

    /// <summary>Whether the database has a capturing secondary as of <paramref name="now"/>.</summary>
    public bool HasCapturingSecondary(string database, DateTime now)
    {
        ArgumentNullException.ThrowIfNull(database);

        lock (_gate)
        {
            if (!_lastSeen.TryGetValue(database, out var seen))
            {
                return false;
            }

            if (now - seen >= Lapse)
            {
                _lastSeen.Remove(database);
                return false;
            }

            return true;
        }
    }

    /// <summary>
    /// Seeds the flag at service start from the store: the databases with a non-primary <c>replica_role</c> row in
    /// <c>query_store_stats</c> in the last 24 hours, with the newest such row's time. Never moves a stamp back.
    /// </summary>
    public void Seed(IEnumerable<(string Database, DateTime LastSeen)> databases)
    {
        ArgumentNullException.ThrowIfNull(databases);

        lock (_gate)
        {
            foreach (var (database, lastSeen) in databases)
            {
                if (!_lastSeen.TryGetValue(database, out var existing) || lastSeen > existing)
                {
                    _lastSeen[database] = lastSeen;
                }
            }
        }
    }

    /// <summary>Drops a database that left the enumeration.</summary>
    public void Forget(string database)
    {
        ArgumentNullException.ThrowIfNull(database);

        lock (_gate)
        {
            _lastSeen.Remove(database);
        }
    }

    /// <summary>The server was removed.</summary>
    public void Clear()
    {
        lock (_gate)
        {
            _lastSeen.Clear();
        }
    }
}
