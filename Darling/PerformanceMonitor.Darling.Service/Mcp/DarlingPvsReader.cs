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

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The ADR persistent version store reads for the MCP/web surface (#2029) — the Viewer's
/// <c>ViewerDataService.FinOps.Pvs.cs</c> reads, mirrored service-side by the same twin convention as
/// <see cref="DarlingPlanCorrectionReader"/>. The latest read is the FinOps grid's newest-collection
/// snapshot, one row per database; the trend read is the #2018 chart's window — every stored point for the
/// TOP-5 databases by PVS size at the newest collection, with percent-of-database computed per POINT from
/// the same row's data-file denominator the grid uses, so no surface can tell a different story.
///
/// <para><b>The four cleaner times are de-skewed to naive UTC at this boundary.</b>
/// <c>PvsStatsCollector</c> ships them verbatim off <c>sys.dm_tran_persistent_version_store_stats</c>, so the
/// stored values are the monitored server's LOCAL wall clock, while the <c>collection_time</c> beside them —
/// and the <c>as_of</c> the tool derives from it — are naive UTC. Left raw, the payload says the off-row
/// cleaner last ran one whole offset ago: measured on the production fleet the cleaner had run 0.6 to 41
/// seconds before the snapshot and the payload read 4 h 00 m. That lands exactly on the question this read
/// exists to answer — the tool's own description sends a caller here "when ADR cleanup looks stuck" — so a
/// phantom four-hour stall is the worst place for an unmarked frame. The trend read needs nothing: it
/// returns only <c>collection_time</c>.</para>
///
/// <para><b>Exact only inside the current DST period.</b> The collected offset is the one in force at
/// collection time, so a cleaner timestamp from before the most recent transition is de-skewed by an
/// offset that was not in force when it was written and lands 60 minutes early. The off-row cleaner runs
/// continuously and is unaffected in practice, but <c>aborted_version_cleaner_*</c> can reach back weeks
/// on a quiet database — a live read showed 2026-08-26 against a 2026-09-09 snapshot — so it can cross a
/// transition. Same limit and same proper fix as <c>DarlingObjectStatsReader</c> records: a collected zone
/// plus <c>AT TIME ZONE</c>, rather than one offset applied to every age of value.</para>
/// </summary>
internal static class DarlingPvsReader
{
    /// <summary>Latest PVS snapshot, one row per database, biggest version store first, with the four cleaner
    /// times de-skewed from the server's local clock to naive UTC. The snapshot self-subquery and the ordering
    /// both stay off the cleaner columns, so neither depends on the offset. $1 server_id.</summary>
    public const string PvsStatsLatestSql = @"
WITH svr AS (
    SELECT COALESCE((
        SELECT sp.utc_offset_minutes
        FROM server_properties AS sp
        WHERE sp.server_id = $1
        AND   sp.utc_offset_minutes IS NOT NULL
        ORDER BY sp.collection_time DESC
        LIMIT 1), 0) AS offset_minutes
)
SELECT
    database_name,
    is_accelerated_database_recovery_on,
    persistent_version_store_size_mb,
    online_index_version_store_size_mb,
    database_data_size_mb,
    current_aborted_transaction_count,
    oldest_active_transaction_id,
    oldest_aborted_transaction_id,
    aborted_version_cleaner_start_time - make_interval(mins => svr.offset_minutes) AS aborted_version_cleaner_start_time,
    aborted_version_cleaner_end_time - make_interval(mins => svr.offset_minutes) AS aborted_version_cleaner_end_time,
    offrow_version_cleaner_start_time - make_interval(mins => svr.offset_minutes) AS offrow_version_cleaner_start_time,
    offrow_version_cleaner_end_time - make_interval(mins => svr.offset_minutes) AS offrow_version_cleaner_end_time,
    collection_time
FROM v_pvs_stats, svr
WHERE server_id = $1
AND   collection_time = (
    SELECT MAX(collection_time)
    FROM v_pvs_stats
    WHERE server_id = $1
)
ORDER BY persistent_version_store_size_mb DESC NULLS LAST, database_name";

    /// <summary>The #2018 trend window for the TOP-5 databases by newest PVS size, per-point
    /// percent-of-database. $1 server_id, $2 window start (naive UTC).</summary>
    public const string PvsTrendSql = @"
WITH top_dbs AS (
    SELECT database_name
    FROM v_pvs_stats
    WHERE server_id = $1
    AND   collection_time = (
        SELECT MAX(collection_time)
        FROM v_pvs_stats
        WHERE server_id = $1
    )
    ORDER BY persistent_version_store_size_mb DESC NULLS LAST, database_name
    LIMIT 5
)
SELECT
    p.database_name,
    p.collection_time,
    p.persistent_version_store_size_mb,
    CASE WHEN p.database_data_size_mb > 0
         THEN p.persistent_version_store_size_mb / p.database_data_size_mb * 100.0
    END AS pct_of_database
FROM v_pvs_stats p
JOIN top_dbs t ON t.database_name = p.database_name
WHERE p.server_id = $1
AND   p.collection_time >= $2
ORDER BY p.database_name, p.collection_time";

    /// <summary>One database's newest PVS snapshot row.</summary>
    public sealed record PvsStatsRow(
        string DatabaseName,
        bool? IsAdrOn,
        double? PvsSizeMb,
        double? OnlineIndexVersionStoreMb,
        double? DatabaseDataSizeMb,
        long? AbortedTransactionCount,
        long? OldestActiveTransactionId,
        long? OldestAbortedTransactionId,
        DateTime? AbortedCleanerStartTimeUtc,
        DateTime? AbortedCleanerEndTimeUtc,
        DateTime? OffrowCleanerStartTimeUtc,
        DateTime? OffrowCleanerEndTimeUtc,
        DateTime CollectionTime);

    /// <summary>One trend point (per database, per collection).</summary>
    public sealed record PvsTrendPoint(
        string DatabaseName,
        DateTime CollectionTime,
        double PvsSizeMb,
        double? PctOfDatabase);

    public static async Task<List<PvsStatsRow>> GetPvsStatsLatestAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<PvsStatsRow>();
        await using var command = postgres.CreateCommand(PvsStatsLatestSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue(serverId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PvsStatsRow(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetBoolean(1),
                reader.IsDBNull(2) ? null : Convert.ToDouble(reader.GetValue(2)),
                reader.IsDBNull(3) ? null : Convert.ToDouble(reader.GetValue(3)),
                reader.IsDBNull(4) ? null : Convert.ToDouble(reader.GetValue(4)),
                reader.IsDBNull(5) ? null : reader.GetInt64(5),
                reader.IsDBNull(6) ? null : reader.GetInt64(6),
                reader.IsDBNull(7) ? null : reader.GetInt64(7),
                reader.IsDBNull(8) ? null : reader.GetDateTime(8),
                reader.IsDBNull(9) ? null : reader.GetDateTime(9),
                reader.IsDBNull(10) ? null : reader.GetDateTime(10),
                reader.IsDBNull(11) ? null : reader.GetDateTime(11),
                reader.GetDateTime(12)));
        }

        return rows;
    }

    public static async Task<List<PvsTrendPoint>> GetPvsTrendAsync(
        NpgsqlDataSource postgres, int serverId, DateTime sinceUtc, CancellationToken cancellationToken = default)
    {
        var rows = new List<PvsTrendPoint>();
        await using var command = postgres.CreateCommand(PvsTrendSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(sinceUtc, DateTimeKind.Unspecified));

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PvsTrendPoint(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.GetDateTime(1),
                reader.IsDBNull(2) ? 0 : Convert.ToDouble(reader.GetValue(2)),
                reader.IsDBNull(3) ? null : Convert.ToDouble(reader.GetValue(3))));
        }

        return rows;
    }
}
