/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// The bucketed TempDB usage/size statement text (#4349), pulled out of
    /// <see cref="GetTempDbTrendAsync"/> so its shape is checkable without a live DuckDB, mirroring
    /// Darling's <c>TempDbTrendSql</c>. Every gauge is AVERAGED per bucket (#3540's rated split does not
    /// apply — none of these columns are deltas). <c>top_session_id</c>/<c>top_session_tempdb_mb</c> are
    /// NOT averaged (a session ID average is meaningless); both take the bucket's LAST raw collection's
    /// pair together via <c>arg_max</c>. $1 server_id, $2/$3 the UTC window, $4 bucket width minutes.
    /// </summary>
    internal static string TempDbTrendSql => $@"
WITH per_collection AS
(
    SELECT
        collection_time,
        user_object_reserved_mb,
        internal_object_reserved_mb,
        version_store_reserved_mb,
        total_reserved_mb,
        unallocated_mb,
        total_sessions_using_tempdb,
        top_session_id,
        top_session_tempdb_mb
    FROM v_tempdb_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
)
SELECT
    GREATEST(time_bucket(to_minutes(CAST($4 AS INTEGER)), collection_time, {TrendBuckets.OriginSql}), $2) AS bucket_start,
    AVG(user_object_reserved_mb) AS user_object_reserved_mb,
    AVG(internal_object_reserved_mb) AS internal_object_reserved_mb,
    AVG(version_store_reserved_mb) AS version_store_reserved_mb,
    AVG(total_reserved_mb) AS total_reserved_mb,
    AVG(unallocated_mb) AS unallocated_mb,
    AVG(total_sessions_using_tempdb) AS total_sessions_using_tempdb,
    arg_max(top_session_id, collection_time) AS top_session_id,
    arg_max(top_session_tempdb_mb, collection_time) AS top_session_tempdb_mb,
    MIN(collection_time) AS first_collection_time,
    COUNT(*) AS collection_count
FROM per_collection
GROUP BY 1
ORDER BY 1";

    /// <summary>
    /// Gets TempDB stats trend for charting. Bucketed to <see cref="TrendBudget.Chart"/>'s point budget
    /// (#4349); a bucket holding exactly one physical collection is stamped at that collection's own raw
    /// time rather than the bucket grid when EVERY bucket this call returned is such a singleton (ruling
    /// item 3).
    /// </summary>
    public async Task<List<TempDbRow>> GetTempDbTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc, SelectedServerTabUtcOffsetMinutes);

        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endTime - startTime).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        command.CommandText = TempDbTrendSql;

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = bucketMinutes });

        var rows = new List<(DateTime BucketStart, DateTime FirstCollectionTime, double UserObjectReservedMb, double InternalObjectReservedMb, double VersionStoreReservedMb, double TotalReservedMb, double UnallocatedMb, int TotalSessionsUsingTempDb, int TopSessionId, double TopSessionTempDbMb, long CollectionCount)>();
        var everyBucketSingleton = true;

        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var collectionCount = reader.GetInt64(10);
            if (collectionCount != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.GetDateTime(0),
                reader.GetDateTime(9),
                reader.IsDBNull(1) ? 0 : ToDouble(reader.GetValue(1)),
                reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
                reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3)),
                reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                reader.IsDBNull(5) ? 0 : ToDouble(reader.GetValue(5)),
                reader.IsDBNull(6) ? 0 : (int)ToInt64(reader.GetValue(6)),
                reader.IsDBNull(7) ? 0 : reader.GetInt32(7),
                reader.IsDBNull(8) ? 0 : ToDouble(reader.GetValue(8)),
                collectionCount));
        }

        var items = new List<TempDbRow>(rows.Count);
        foreach (var row in rows)
        {
            items.Add(new TempDbRow
            {
                CollectionTime = everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                UserObjectReservedMb = row.UserObjectReservedMb,
                InternalObjectReservedMb = row.InternalObjectReservedMb,
                VersionStoreReservedMb = row.VersionStoreReservedMb,
                TotalReservedMb = row.TotalReservedMb,
                UnallocatedMb = row.UnallocatedMb,
                TotalSessionsUsingTempDb = row.TotalSessionsUsingTempDb,
                TopSessionId = row.TopSessionId,
                TopSessionTempDbMb = row.TopSessionTempDbMb
            });
        }

        return items;
    }

    /// <summary>
    /// Gets the latest TempDB space snapshot for alert checking. <c>collection_time</c> rides as the last column
    /// since #3653 (A5): the row the read already orders by, projected so the shared engine's tempdb persistence
    /// gate can tell a fresh collection from a re-read of the last one — see
    /// <see cref="TempDbSpaceInfo.CollectionTimeUtc"/>. Darling's <c>TempDbSpaceSql</c> twin projects the same.
    /// </summary>
    public async Task<TempDbSpaceInfo?> GetLatestTempDbSpaceAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
SELECT
    total_reserved_mb,
    unallocated_mb,
    user_object_reserved_mb,
    internal_object_reserved_mb,
    version_store_reserved_mb,
    top_session_tempdb_mb,
    top_session_id,
    max_size_mb,
    collection_time
FROM v_tempdb_stats
WHERE server_id = $1
ORDER BY collection_time DESC
LIMIT 1";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        using var reader = await command.ExecuteReaderAsync();
        if (await reader.ReadAsync())
        {
            return new TempDbSpaceInfo
            {
                TotalReservedMb = reader.IsDBNull(0) ? 0 : ToDouble(reader.GetValue(0)),
                UnallocatedMb = reader.IsDBNull(1) ? 0 : ToDouble(reader.GetValue(1)),
                UserObjectReservedMb = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
                InternalObjectReservedMb = reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3)),
                VersionStoreReservedMb = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                TopConsumerMb = reader.IsDBNull(5) ? 0 : ToDouble(reader.GetValue(5)),
                TopConsumerSessionId = reader.IsDBNull(6) ? 0 : reader.GetInt32(6),
                /* NULL on every row collected before the v56 migration, and 0 is what "no ceiling
                   measured" is spelled as — so history keeps reporting the percentage it always did
                   rather than dividing by a zero cap. Darling's twin reads the same column. */
                MaxSizeMb = reader.IsDBNull(7) ? 0 : ToDouble(reader.GetValue(7)),
                /* #3653 (A5): the collector stamps UTC into DuckDB's tz-less TIMESTAMP, so the value comes back
                   Kind Unspecified and is stamped Utc because that is what it is — the AG-topology reads' idiom.
                   The engine only ever compares one server's stamps with each other. */
                CollectionTimeUtc = reader.IsDBNull(8) ? null : DateTime.SpecifyKind(reader.GetDateTime(8), DateTimeKind.Utc)
            };
        }

        return null;
    }
}

/* TempDbSpaceInfo moved to PerformanceMonitor.Alerting (Phase-5 A0);
   the bare name resolves through the global using alias in GlobalUsings.cs. */

public class TempDbRow
{
    public DateTime CollectionTime { get; set; }
    public double UserObjectReservedMb { get; set; }
    public double InternalObjectReservedMb { get; set; }
    public double VersionStoreReservedMb { get; set; }
    public double TotalReservedMb { get; set; }
    public double UnallocatedMb { get; set; }
    public int TotalSessionsUsingTempDb { get; set; }
    public int TopSessionId { get; set; }
    public double TopSessionTempDbMb { get; set; }
}
