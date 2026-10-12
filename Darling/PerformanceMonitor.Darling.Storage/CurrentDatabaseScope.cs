/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #5633 item 4: which databases a server has NOW, so a read over stored history can leave out a database that was
/// dropped. The set is the newest <c>database_states</c> snapshot for the server, the same source and the same SQL text
/// (<see cref="QueryStoreBackfillState.NewestSnapshotSql"/>) that #5486's gone-database check in the Query Store
/// backfill reads, not a second list.
///
/// <para><b>Unknown is not empty.</b> <see cref="ReadAsync"/> returns <c>null</c> when the server has no snapshot (an older
/// store, an Azure SQL Database, where the collector does not apply), when the newest one is older than
/// <see cref="SnapshotHorizon"/> (a collector that stopped: its list still names a database dropped since), when it names
/// no database, or when the read fails. Every caller treats <c>null</c> as "filter nothing".</para>
///
/// <para><b>Plan cost.</b> One indexed read per call: <c>MAX(collection_time)</c> over
/// <c>idx_database_states_time (server_id, collection_time)</c> bounded below by the horizon, then the few rows (one per
/// database) at that stamp. It never touches the fact tables the reads filter.</para>
///
/// <para>A database created after the newest snapshot is out of the set until the next snapshot (the collector runs every
/// 30 minutes by default). The backfill's extra "any row since the snapshot" guard is not applied here: it would put a
/// fact-table probe in every read.</para>
/// </summary>
public static class CurrentDatabaseScope
{
    /// <summary>How old the newest snapshot may be and still decide which databases are gone: six hours, several times
    /// the default 30-minute interval, so a server whose interval was raised to an hour or two still counts as watched.</summary>
    public static readonly TimeSpan SnapshotHorizon = TimeSpan.FromHours(6);

    /// <summary>
    /// The databases the server's newest <c>database_states</c> snapshot names, or <c>null</c> when that is unknown.
    /// Never throws except for cancellation: a failed read is "unknown", so a monitoring read never fails on this lookup.
    /// </summary>
    public static async Task<IReadOnlySet<string>?> ReadAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default, DateTime? nowUtc = null)
    {
        try
        {
            var floor = DateTime.SpecifyKind((nowUtc ?? DateTime.UtcNow) - SnapshotHorizon, DateTimeKind.Unspecified);
            await using var command = postgres.CreateCommand(QueryStoreBackfillState.NewestSnapshotSql);
            command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(floor);
            var names = new HashSet<string>(StringComparer.Ordinal);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                if (!reader.IsDBNull(0))
                {
                    names.Add(reader.GetString(0));
                }
            }

            return names.Count == 0 ? null : names;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }
    }

    /// <summary>True when <paramref name="current"/> is known and does not name <paramref name="databaseName"/>.
    /// Ordinal, as the backfill's check is.</summary>
    public static bool IsDropped(IReadOnlySet<string>? current, string? databaseName)
        => current is not null && databaseName is not null && !current.Contains(databaseName);

    /// <summary>
    /// <paramref name="filter"/> narrowed to the current databases when it chose none (<see cref="DatabaseFilter.IsAll"/>)
    /// and the set is known; otherwise the filter unchanged. A database the caller names is read as asked: naming a
    /// dropped database is a question about its history, and the filter answers it.
    /// </summary>
    public static DatabaseFilter Narrow(DatabaseFilter filter, IReadOnlySet<string>? current)
        => filter.IsAll && current is not null ? DatabaseFilter.Of(current) : filter;

    /// <summary><see cref="Narrow(DatabaseFilter, IReadOnlySet{string})"/> after reading the server's set.</summary>
    public static async Task<DatabaseFilter> NarrowAsync(
        NpgsqlDataSource postgres, int serverId, DatabaseFilter filter, CancellationToken cancellationToken = default)
        => filter.IsAll ? Narrow(filter, await ReadAsync(postgres, serverId, cancellationToken)) : filter;

    /// <summary>
    /// <paramref name="rows"/> without the rows of a database its server no longer has, for a read that lists per-database
    /// rows and chose none (<paramref name="filter"/> is <see cref="DatabaseFilter.IsAll"/>). The rows are OMITTED, not
    /// marked, because the verdict counts and the recommendations built from them must not count a dropped database.
    /// One <see cref="ReadAsync"/> per distinct server in the rows; a server whose set is unknown keeps its rows.
    /// </summary>
    public static async Task<List<T>> OmitDroppedAsync<T>(
        NpgsqlDataSource postgres, List<T> rows, Func<T, int> serverOf, Func<T, string?> databaseOf, DatabaseFilter filter,
        CancellationToken cancellationToken = default)
    {
        if (!filter.IsAll || rows.Count == 0)
        {
            return rows;
        }

        var sets = new Dictionary<int, IReadOnlySet<string>?>();
        foreach (var row in rows)
        {
            var serverId = serverOf(row);
            if (!sets.ContainsKey(serverId))
            {
                sets[serverId] = await ReadAsync(postgres, serverId, cancellationToken);
            }
        }

        return rows.FindAll(row => !IsDropped(sets[serverOf(row)], databaseOf(row)));
    }
}
