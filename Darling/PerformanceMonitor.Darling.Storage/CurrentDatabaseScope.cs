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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
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
/// 30 minutes by default). <see cref="OmitDroppedAsync"/> keeps a row stamped after the snapshot's own time (no extra query);
/// the backfill's per-database fact-table probe is not applied here. <see cref="NarrowAsync"/>, which filters by name BEFORE a
/// ranking read, does not use the newest snapshot's names as an include list (that hid a new database until the next snapshot): it
/// excludes only names an earlier snapshot listed that the newest does not (<see cref="ReadDroppedAsync"/>), so a name no
/// snapshot ever listed is read.</para>
///
/// <para><b>Historical reads.</b> With an <c>as_of</c> the set is the newest snapshot at or before it, with the horizon measured
/// from it (#5633 review M2), so a window that ended before a drop still sees the database.</para>
/// </summary>
public static class CurrentDatabaseScope
{
    /// <summary>How old the newest snapshot may be and still decide which databases are gone: six hours, several times
    /// the default 30-minute interval, so a server whose interval was raised to an hour or two still counts as watched.</summary>
    public static readonly TimeSpan SnapshotHorizon = TimeSpan.FromHours(6);

    /// <summary>The databases of one <c>database_states</c> snapshot and when it was taken (its <c>collection_time</c>).</summary>
    public sealed record Snapshot(IReadOnlySet<string> Databases, DateTime TakenAt);

    /// <summary>
    /// The databases the server's newest <c>database_states</c> snapshot names, or <c>null</c> when that is unknown.
    /// Never throws except for cancellation: a failed read is "unknown", so a monitoring read never fails on this lookup.
    /// <paramref name="asOfUtc"/> (#5633 review M2) is a historical read's anchor: the newest snapshot AT OR BEFORE it, with the
    /// horizon measured from it, so a database dropped after the window is still in the set for a window it existed in.
    /// </summary>
    public static async Task<IReadOnlySet<string>?> ReadAsync(
        NpgsqlDataSource postgres, int serverId, DateTime? nowUtc = null, DateTime? asOfUtc = null, ILogger? logger = null,
        CancellationToken cancellationToken = default)
        => (await ReadSnapshotAsync(postgres, serverId, nowUtc, asOfUtc, logger, cancellationToken))?.Databases;

    /// <summary>
    /// <see cref="ReadAsync"/> with the snapshot's own collection time (#5633 review M2): the time lets a caller keep a row stamped
    /// after the snapshot (a database created since it) instead of treating it as dropped. Same unknown rules and same read.
    /// </summary>
    public static async Task<Snapshot?> ReadSnapshotAsync(
        NpgsqlDataSource postgres, int serverId, DateTime? nowUtc = null, DateTime? asOfUtc = null, ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var anchor = asOfUtc ?? nowUtc ?? DateTime.UtcNow;
            var floor = DateTime.SpecifyKind(anchor - SnapshotHorizon, DateTimeKind.Unspecified);
            await using var command = postgres.CreateCommand(asOfUtc is null ? QueryStoreBackfillState.NewestSnapshotSql : QueryStoreBackfillState.NewestSnapshotAtOrBeforeSql);
            command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(floor);
            if (asOfUtc is { } ceiling)
            {
                command.Parameters.AddWithValue(DateTime.SpecifyKind(ceiling, DateTimeKind.Unspecified));
            }

            var names = new HashSet<string>(StringComparer.Ordinal);
            DateTime takenAt = default;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                if (!reader.IsDBNull(0))
                {
                    names.Add(reader.GetString(0));
                    takenAt = reader.GetDateTime(1);
                }
            }

            return names.Count == 0 ? null : new Snapshot(names, takenAt);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            WarnFailOpen(logger, "newest database_states snapshot", serverId, ex);
            return null;
        }
    }

    /// <summary>True when <paramref name="current"/> is known and does not name <paramref name="databaseName"/>.
    /// Ordinal, as the backfill's check is.</summary>
    public static bool IsDropped(IReadOnlySet<string>? current, string? databaseName)
        => current is not null && databaseName is not null && !current.Contains(databaseName);

    /// <summary>The longest look-back <see cref="ReadDroppedAsync"/> reaches for an earlier snapshot naming a database: a ranking
    /// window older than this finds a drop only inside this span. Seven days is far beyond the 6-hour newest-snapshot horizon, so a
    /// database dropped during any ordinary ranking window is found, and it bounds the work to about 336 snapshots at 30 minutes
    /// (one row per database each, grouped by index).</summary>
    public static readonly TimeSpan DropLookBack = TimeSpan.FromDays(7);

    /// <summary>
    /// #5633 review: the databases the server DROPPED, for a ranking read that must leave them out: names an earlier
    /// <c>database_states</c> snapshot lists that the newest one does not. A name no snapshot ever lists (a database created after the
    /// newest snapshot, or one the snapshots never cover) is not in the set, so it is never left out; this replaces narrowing to the
    /// newest snapshot's names, which hid a new database until the next snapshot listed it. One grouped read of <c>database_states</c> over
    /// <c>idx_database_states_time</c>, from the window's start (<paramref name="windowStartUtc"/>) back no further than
    /// <see cref="DropLookBack"/> and no nearer than <see cref="SnapshotHorizon"/>; no fact table is touched.
    /// Null is unknown (no snapshot, the newest older than <see cref="SnapshotHorizon"/>, a failed read); an empty set is "nothing
    /// dropped". <paramref name="asOfUtc"/> anchors a historical read as in <see cref="ReadSnapshotAsync"/>.
    /// </summary>
    public static async Task<IReadOnlySet<string>?> ReadDroppedAsync(
        NpgsqlDataSource postgres, int serverId, DateTime? nowUtc = null, DateTime? asOfUtc = null, DateTime? windowStartUtc = null,
        ILogger? logger = null, CancellationToken cancellationToken = default)
    {
        try
        {
            var anchor = asOfUtc ?? nowUtc ?? DateTime.UtcNow;
            var floor = LookBackFloor(anchor, windowStartUtc);
            await using var command = postgres.CreateCommand(asOfUtc is null ? QueryStoreBackfillState.LastSeenPerDatabaseSql : QueryStoreBackfillState.LastSeenPerDatabaseAtOrBeforeSql);
            command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(DateTime.SpecifyKind(floor, DateTimeKind.Unspecified));
            if (asOfUtc is { } ceiling)
            {
                command.Parameters.AddWithValue(DateTime.SpecifyKind(ceiling, DateTimeKind.Unspecified));
            }

            var lastSeen = new Dictionary<string, DateTime>(StringComparer.Ordinal);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                if (!reader.IsDBNull(0) && !reader.IsDBNull(1))
                {
                    lastSeen[reader.GetString(0)] = reader.GetDateTime(1);
                }
            }

            return DroppedFrom(lastSeen, anchor);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            WarnFailOpen(logger, "last-seen database_states read", serverId, ex);
            return null;
        }
    }

    /// <summary>
    /// #5633 review: the fail-open is kept (a monitoring read never fails on this lookup), but a SQL or ordinal mistake in it
    /// turns all of item 4 off, so each failure is logged once per occurrence at Warning, the way the rest of this assembly logs a
    /// best-effort step that gave up (<c>logger?.LogWarning</c> with the exception message; a caller with no logger passes none).
    /// </summary>
    private static void WarnFailOpen(ILogger? logger, string what, int serverId, Exception ex)
        => logger?.LogWarning(
            "CurrentDatabaseScope: the {What} for server {ServerId} failed, so no dropped database is left out of the read that asked: {Message}",
            what, serverId, ex.Message);

    /// <summary>The oldest snapshot stamp <see cref="ReadDroppedAsync"/> reads: the window's start when it is given, never nearer than
    /// <see cref="SnapshotHorizon"/> before the anchor and never further than <see cref="DropLookBack"/>.</summary>
    internal static DateTime LookBackFloor(DateTime anchorUtc, DateTime? windowStartUtc)
    {
        var floor = anchorUtc - SnapshotHorizon;
        if (windowStartUtc is { } start && start < floor)
        {
            floor = start;
        }

        var oldest = anchorUtc - DropLookBack;
        return floor < oldest ? oldest : floor;
    }

    /// <summary>The pure half of <see cref="ReadDroppedAsync"/>: from each database's newest stamp, the names whose newest stamp is older
    /// than the newest stamp overall. Null when there is no stamp or the newest is not within <see cref="SnapshotHorizon"/> of the anchor
    /// (a collector that stopped: nothing it lists can be called dropped).</summary>
    internal static IReadOnlySet<string>? DroppedFrom(IReadOnlyDictionary<string, DateTime> lastSeenPerDatabase, DateTime anchorUtc)
    {
        if (lastSeenPerDatabase.Count == 0)
        {
            return null;
        }

        var newest = lastSeenPerDatabase.Values.Max();
        if (newest <= anchorUtc - SnapshotHorizon)
        {
            return null;
        }

        return lastSeenPerDatabase.Where(pair => pair.Value < newest).Select(pair => pair.Key).ToHashSet(StringComparer.Ordinal);
    }

    /// <summary>
    /// <paramref name="filter"/> with the dropped databases left out when it chose none (<see cref="DatabaseFilter.IsAll"/>) and some
    /// are known (<see cref="DatabaseFilter.Excluding"/>); otherwise the filter unchanged. A database the caller names is read as
    /// asked: naming a dropped database is a question about its history, and the filter answers it.
    /// </summary>
    public static DatabaseFilter Narrow(DatabaseFilter filter, IReadOnlySet<string>? dropped)
        => filter.IsAll && dropped is { Count: > 0 } ? DatabaseFilter.Excluding(dropped) : filter;

    /// <summary><see cref="Narrow(DatabaseFilter, IReadOnlySet{string})"/> after reading the server's dropped set
    /// (<see cref="ReadDroppedAsync"/>). The reader the filter goes to must apply <see cref="DatabaseFilter.ExcludeClause(string, int)"/>.</summary>
    public static async Task<DatabaseFilter> NarrowAsync(
        NpgsqlDataSource postgres, int serverId, DatabaseFilter filter, DateTime? nowUtc = null, DateTime? asOfUtc = null,
        DateTime? windowStartUtc = null, ILogger? logger = null, CancellationToken cancellationToken = default)
        => filter.IsAll ? Narrow(filter, await ReadDroppedAsync(postgres, serverId, nowUtc, asOfUtc, windowStartUtc, logger, cancellationToken)) : filter;

    /// <summary>
    /// <paramref name="rows"/> without the rows of a database its server no longer has, for a read that lists per-database
    /// rows and chose none (<paramref name="filter"/> is <see cref="DatabaseFilter.IsAll"/>). The rows are OMITTED, not
    /// marked, because the verdict counts and the recommendations built from them must not count a dropped database.
    /// One <see cref="ReadSnapshotAsync"/> per distinct server in the rows; a server whose set is unknown keeps its rows.
    /// <para><paramref name="asOfUtc"/> (#5633 review M3) is a historical read's anchor, passed to <see cref="ReadSnapshotAsync"/>: the
    /// newest snapshot at or before it, so a database dropped after the window is kept for a window it existed in.</para>
    /// <para><paramref name="omittedDatabases"/> (#5633 review, finding D) receives the name of every database whose rows were left out, so
    /// the page a human reads can say what it left out instead of reading as "nothing there". Null collects nothing.</para>
    /// <para><paramref name="timeOf"/> (#5633 review M2): a row stamped AFTER the snapshot was taken is kept, because its database
    /// may have been created since (the snapshot is up to 30 minutes old). It costs no extra query. Null keeps no such row.</para>
    /// </summary>
    public static async Task<List<T>> OmitDroppedAsync<T>(
        NpgsqlDataSource postgres, List<T> rows, Func<T, int> serverOf, Func<T, string?> databaseOf, DatabaseFilter filter,
        Func<T, DateTime?>? timeOf = null, DateTime? asOfUtc = null, ICollection<string>? omittedDatabases = null, ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        if (!filter.IsAll || rows.Count == 0)
        {
            return rows;
        }

        var sets = new Dictionary<int, Snapshot?>();
        foreach (var row in rows)
        {
            var serverId = serverOf(row);
            if (!sets.ContainsKey(serverId))
            {
                sets[serverId] = await ReadSnapshotAsync(postgres, serverId, asOfUtc: asOfUtc, logger: logger, cancellationToken: cancellationToken);
            }
        }

        return rows.FindAll(row =>
        {
            var snapshot = sets[serverOf(row)];
            if (!IsDropped(snapshot?.Databases, databaseOf(row)))
            {
                return true;
            }

            if (snapshot is not null && timeOf?.Invoke(row) is { } rowTime && rowTime > snapshot.TakenAt)
            {
                return true;
            }

            omittedDatabases?.Add(databaseOf(row)!);
            return false;
        });
    }
}
