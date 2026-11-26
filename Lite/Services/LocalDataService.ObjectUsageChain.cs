/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// The persisted read behind Lite's <c>get_object_usage</c> (#9596, twinning Darling's <c>DarlingObjectUsageReader</c> of #9349). The window is
/// replayed over the chain's per-snapshot facts (<see cref="ObjectUsageWindowReplay"/>, shared with Darling) and the stored deltas are summed,
/// so the read touches one row per snapshot and the moved indexes instead of every index row of every snapshot. Whenever the chain cannot prove
/// the full fold's answer (no chain yet, a chain behind the newest snapshot, an older step version, a step the window decides differently, a
/// stored row missing) <see cref="GetObjectUsageWindowAsync"/> takes the full fold, so the answer never differs from it.
/// </summary>
public partial class LocalDataService
{
    /// <summary>
    /// The window's baseline, the newest raw snapshot of one database, and how many distinct snapshots lie between them, both ends included. The
    /// baseline is the snapshot <see cref="ObjectUsageWindowRead.Sql"/> starts from: <see cref="ObjectUsageWindowRead.BaselineCte"/> itself, else
    /// the first snapshot at or after the start. The newest snapshot is read only from the baseline's earliest possible time onward, so the archive
    /// files before the window stay closed. The count is what the chain's facts must equal (#9596): a snapshot that lands at or before the chain's
    /// newest time (a late write, a clock stepped back) is a hole the facts' own links do not show. $1 server_id, $2 database_name, $3 the window start.
    /// </summary>
    internal static readonly string ObjectUsageBoundsSql = ObjectUsageWindowRead.BaselineCte + $"""
    ,
    span AS
    (
        SELECT
            (
                SELECT min(s.collection_time)
                FROM v_index_object_stats AS s
                WHERE s.server_id = $1
                AND   s.database_name = $2
                AND   s.collection_time >= COALESCE((SELECT collection_time FROM baseline), $3)
            ) AS first_time,
            (
                SELECT max(n.collection_time)
                FROM v_index_object_stats AS n
                WHERE n.server_id = $1
                AND   n.database_name = $2
                AND   n.collection_time > $3 - interval '{ObjectUsageWindowRead.BaselineReachDays} days'
            ) AS last_time
    )
    SELECT
        span.first_time,
        span.last_time,
        (
            SELECT count(DISTINCT c.collection_time)
            FROM v_index_object_stats AS c
            WHERE c.server_id = $1
            AND   c.database_name = $2
            AND   c.collection_time >= span.first_time
            AND   c.collection_time <= span.last_time
        )
    FROM span
    """;

    /// <summary>The chain's per-snapshot facts of one database from the baseline to the newest snapshot, oldest first. $1 server_id, $2 database_name,
    /// $3 the baseline, $4 the newest snapshot.</summary>
    internal const string ObjectUsageFactsSql = """
        SELECT
            f.collection_time,
            f.step_version,
            f.answering_server_name,
            f.sqlserver_start_time,
            f.compared_with,
            f.reasons,
            f.index_resets,
            f.blind_from
        FROM object_usage_snapshots AS f
        WHERE f.server_id = $1
        AND   f.database_name = $2
        AND   f.collection_time >= $3
        AND   f.collection_time <= $4
        ORDER BY
            f.collection_time
        """;

    /// <summary>The stored deltas of the window summed per object, the baseline left out. $1 server_id, $2 database_name, $3 the baseline,
    /// $4 the newest snapshot. The caller appends the boundary steps' times as <c>AND d.collection_time NOT IN (...)</c> (<see cref="ObjectUsageDeltasSql"/>).</summary>
    internal const string ObjectUsageDeltasBaseSql = """
        SELECT
            d.object_id,
            CAST(sum(d.reads) AS BIGINT),
            COALESCE(CAST(sum(d.updates) FILTER (WHERE d.index_id <= 1) AS BIGINT), 0),
            max(d.last_read),
            max(d.last_update)
        FROM object_usage_deltas AS d
        WHERE d.server_id = $1
        AND   d.database_name = $2
        AND   d.collection_time > $3
        AND   d.collection_time <= $4
        """;

    private const string ObjectUsageDeltasGroupBy = """

        GROUP BY
            d.object_id
        """;

    /// <summary>The deltas statement with the boundary steps' times left out: <c>NOT IN</c> over parameters from $5 on.</summary>
    internal static string ObjectUsageDeltasSql(int boundaryCount) =>
        ObjectUsageDeltasBaseSql + (boundaryCount == 0 ? "" : "\n        AND   d.collection_time NOT IN (" + ParameterList(5, boundaryCount) + ")") + ObjectUsageDeltasGroupBy;

    /// <summary>The runs of one database that overlap the window. $1 server_id, $2 database_name, $3 the baseline, $4 the newest snapshot.</summary>
    internal const string ObjectUsageRunsSql = """
        SELECT
            r.object_id,
            r.index_id,
            r.first_time,
            r.last_time,
            r.schema_name,
            r.object_name,
            r.is_indexed_view,
            r.is_memory_optimized
        FROM object_usage_runs AS r
        WHERE r.server_id = $1
        AND   r.database_name = $2
        AND   r.first_time <= $4
        AND   (r.last_time IS NULL OR r.last_time >= $3)
        ORDER BY
            r.first_time,
            r.index_id
        """;

    /// <summary>The raw rows of the snapshots the replay recomputes, <see cref="ObjectUsageWindowRead.Columns"/> so <see cref="ObjectUsageWindowRead.Map"/> reads them.
    /// $1 server_id, $2 database_name, the collection times from $3 on.</summary>
    internal static string ObjectUsageBoundaryRowsSql(int timeCount) =>
        "SELECT " + ObjectUsageWindowRead.Columns + "\n        FROM v_index_object_stats AS s\n        WHERE s.server_id = $1\n        AND   s.database_name = $2\n        AND   s.collection_time IN ("
        + ParameterList(3, timeCount) + ")";

    private static string ParameterList(int first, int count) =>
        string.Join(", ", Enumerable.Range(first, count).Select(n => "$" + n.ToString(CultureInfo.InvariantCulture)));

    /// <summary>Reads the window for one database and runs it through <see cref="ObjectUsageWindow"/>: from the persisted chain where it can prove
    /// the full fold's answer, from every snapshot otherwise.</summary>
    public async Task<ObjectUsageWindowResult> GetObjectUsageWindowAsync(int serverId, string databaseName, DateTime windowStartUtc)
    {
        try
        {
            var (result, reason) = await TryReadObjectUsageChainAsync(serverId, databaseName, windowStartUtc);
            if (result is not null)
            {
                return result;
            }

            AppLogger.Debug("ObjectUsage", $"get_object_usage took the full fold for server {serverId}: {reason}");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* A chain table that cannot be read (a v87 migration that failed, a file mid-rebuild) costs the speed, never the answer. */
            AppLogger.Warn("ObjectUsage", $"get_object_usage could not read the object usage chain and took the full fold: {ex.Message}");
        }

        return await GetObjectUsageWindowFullAsync(serverId, databaseName, windowStartUtc);
    }

    /// <summary>The persisted path. A null result comes with the reason the window cannot be answered from the chain.</summary>
    internal async Task<(ObjectUsageWindowResult? Result, string? Reason)> TryReadObjectUsageChainAsync(
        int serverId, string databaseName, DateTime windowStartUtc)
    {
        var start = DateTime.SpecifyKind(windowStartUtc, DateTimeKind.Unspecified);
        DateTime? baseline = null;
        DateTime? newest = null;
        long snapshotCount = 0;
        var facts = new List<ObjectUsageSnapshotFact>();
        var deltas = new List<ObjectUsageDeltaTotals>();
        var runs = new List<ObjectUsageRun>();
        var raw = new List<ObjectUsageSample>();

        using var connection = await OpenConnectionAsync();

        /* One snapshot of the chain tables and the raw rows for every statement below: the builder commits a step, a rebuild or a purge under the
           same read lock, and a read that straddled one would put the facts of one state beside the deltas of another. The transaction ends
           with the connection, having written nothing. */
        using (var begin = connection.CreateCommand())
        {
            begin.CommandText = "BEGIN TRANSACTION";
            await begin.ExecuteNonQueryAsync();
        }

        using (var command = connection.CreateCommand())
        {
            command.CommandText = ObjectUsageBoundsSql;
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = databaseName });
            command.Parameters.Add(new DuckDBParameter { Value = start });
            using var reader = await command.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                baseline = reader.IsDBNull(0) ? null : reader.GetDateTime(0);
                newest = reader.IsDBNull(1) ? null : reader.GetDateTime(1);
                snapshotCount = reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2), CultureInfo.InvariantCulture);
            }
        }

        if (baseline is not { } from || newest is not { } to)
        {
            return (null, "the window holds no snapshot");
        }

        using (var command = connection.CreateCommand())
        {
            command.CommandText = ObjectUsageFactsSql;
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = databaseName });
            command.Parameters.Add(new DuckDBParameter { Value = from });
            command.Parameters.Add(new DuckDBParameter { Value = to });
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                facts.Add(new ObjectUsageSnapshotFact(
                    reader.GetDateTime(0),
                    Convert.ToInt16(reader.GetValue(1), CultureInfo.InvariantCulture),
                    reader.IsDBNull(2) ? null : reader.GetString(2),
                    reader.IsDBNull(3) ? null : reader.GetDateTime(3),
                    reader.IsDBNull(4) ? null : reader.GetDateTime(4),
                    (ObjectUsageResetReasons)Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture),
                    Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture),
                    reader.IsDBNull(7) ? null : reader.GetDateTime(7)));
            }
        }

        var replay = ObjectUsageWindowReplay.Run(facts, from, to, ObjectUsageChainVersion.Current);
        if (replay.FallbackReason is not null)
        {
            return (null, replay.FallbackReason);
        }

        /* The replay checks the facts' own links, which a hole in the middle keeps intact: the step after it compares the snapshot before it. Only
           the raw store knows a snapshot is missing from the chain, and the full fold steps it. */
        if (facts.Count != snapshotCount)
        {
            return (null, "the chain holds a different number of snapshots than the window");
        }

        var boundaryTimes = replay.Boundaries.Select(b => b.Time).ToList();
        using (var command = connection.CreateCommand())
        {
            command.CommandText = ObjectUsageDeltasSql(boundaryTimes.Count);
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = databaseName });
            command.Parameters.Add(new DuckDBParameter { Value = from });
            command.Parameters.Add(new DuckDBParameter { Value = to });
            foreach (var time in boundaryTimes)
            {
                command.Parameters.Add(new DuckDBParameter { Value = time });
            }

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                deltas.Add(new ObjectUsageDeltaTotals(
                    Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture),
                    Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture),
                    Convert.ToInt64(reader.GetValue(2), CultureInfo.InvariantCulture),
                    reader.IsDBNull(3) ? null : reader.GetDateTime(3),
                    reader.IsDBNull(4) ? null : reader.GetDateTime(4)));
            }
        }

        using (var command = connection.CreateCommand())
        {
            command.CommandText = ObjectUsageRunsSql;
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = databaseName });
            command.Parameters.Add(new DuckDBParameter { Value = from });
            command.Parameters.Add(new DuckDBParameter { Value = to });
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                runs.Add(new ObjectUsageRun(
                    Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture),
                    Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture),
                    reader.GetDateTime(2),
                    reader.IsDBNull(3) ? null : reader.GetDateTime(3),
                    reader.IsDBNull(4) ? "" : reader.GetString(4),
                    reader.IsDBNull(5) ? "" : reader.GetString(5),
                    reader.GetBoolean(6),
                    reader.IsDBNull(7) ? null : reader.GetBoolean(7)));
            }
        }

        var rawTimes = replay.RawTimes;
        using (var command = connection.CreateCommand())
        {
            command.CommandText = ObjectUsageBoundaryRowsSql(rawTimes.Count);
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = databaseName });
            foreach (var time in rawTimes)
            {
                command.Parameters.Add(new DuckDBParameter { Value = time });
            }

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                raw.Add(ObjectUsageWindowRead.Map(reader));
            }
        }

        var result = replay.Assemble(deltas, runs, raw, out var reason);
        return (result, reason);
    }

    /// <summary>The full fold: every index row of every snapshot in the window through <see cref="ObjectUsageWindow"/>. The persisted path's fallback.</summary>
    internal async Task<ObjectUsageWindowResult> GetObjectUsageWindowFullAsync(int serverId, string databaseName, DateTime windowStartUtc)
    {
        var window = new ObjectUsageWindow();
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = ObjectUsageWindowRead.SqlInCollectionIdOrder;
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = databaseName });
        command.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(windowStartUtc, DateTimeKind.Unspecified) });
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            window.Add(ObjectUsageWindowRead.Map(reader));
        }

        return window.Complete();
    }
}
