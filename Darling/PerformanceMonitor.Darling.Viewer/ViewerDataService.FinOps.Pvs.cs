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

namespace PerformanceMonitor.Darling.Viewer;

public sealed partial class ViewerDataService
{
    /// <summary>
    /// Latest ADR persistent version store snapshot, one row per database (#1951). The Postgres twin of
    /// Lite's <c>GetPvsStatsLatestAsync</c> — same columns, same order, same newest-collection pin, so the
    /// two front ends cannot drift.
    /// </summary>
    public const string PvsStatsLatestSql = @"
SELECT
    database_name,
    is_accelerated_database_recovery_on,
    persistent_version_store_size_mb,
    online_index_version_store_size_mb,
    database_data_size_mb,
    current_aborted_transaction_count,
    oldest_active_transaction_id,
    oldest_aborted_transaction_id,
    aborted_version_cleaner_start_time,
    aborted_version_cleaner_end_time,
    offrow_version_cleaner_start_time,
    offrow_version_cleaner_end_time,
    pvs_off_row_page_skipped_low_water_mark,
    pvs_off_row_page_skipped_min_useful_xts,
    pvs_off_row_page_skipped_oldest_aborted_xdesid
FROM v_pvs_stats
WHERE server_id = $1
AND   collection_time = (
    SELECT MAX(collection_time)
    FROM v_pvs_stats
    WHERE server_id = $1
)
ORDER BY persistent_version_store_size_mb DESC NULLS LAST, database_name";

    /// <summary>
    /// #1984 stage 2: the PVS trend behind the FinOps chart — every stored point over the window
    /// for the TOP-5 databases by PVS size at the newest collection (the databases whose growth
    /// story matters; a 90-day retention series for every database of a big instance would swamp
    /// the plot and the read). Percent-of-database is computed per POINT from the same row's data
    /// file denominator, the exact ratio the grid shows, so the two surfaces cannot disagree.
    /// Mirrors Lite's <c>GetPvsTrendAsync</c> — same columns, same top-N pin, same ordering.
    /// </summary>
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

    public async Task<List<PvsTrendPoint>> GetPvsTrendAsync(
        int serverId, DateTime sinceUtc, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(PvsTrendSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(sinceUtc, DateTimeKind.Unspecified) });

        var items = new List<PvsTrendPoint>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new PvsTrendPoint(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.GetDateTime(1),
                reader.IsDBNull(2) ? 0 : Convert.ToDouble(reader.GetValue(2)),
                reader.IsDBNull(3) ? null : Convert.ToDouble(reader.GetValue(3))));
        }

        return items;
    }

    public async Task<List<PvsStatsRow>> GetPvsStatsLatestAsync(int serverId, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(PvsStatsLatestSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });

        var items = new List<PvsStatsRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new PvsStatsRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                IsAdrOn = reader.IsDBNull(1) ? null : reader.GetBoolean(1),
                PvsSizeMb = reader.IsDBNull(2) ? null : Convert.ToDecimal(reader.GetValue(2)),
                OnlineIndexVersionStoreMb = reader.IsDBNull(3) ? null : Convert.ToDecimal(reader.GetValue(3)),
                DatabaseDataSizeMb = reader.IsDBNull(4) ? null : Convert.ToDecimal(reader.GetValue(4)),
                AbortedTransactionCount = reader.IsDBNull(5) ? null : Convert.ToInt64(reader.GetValue(5)),
                OldestActiveTransactionId = reader.IsDBNull(6) ? null : Convert.ToInt64(reader.GetValue(6)),
                OldestAbortedTransactionId = reader.IsDBNull(7) ? null : Convert.ToInt64(reader.GetValue(7)),
                AbortedCleanerStartTime = reader.IsDBNull(8) ? null : reader.GetDateTime(8),
                AbortedCleanerEndTime = reader.IsDBNull(9) ? null : reader.GetDateTime(9),
                OffrowCleanerStartTime = reader.IsDBNull(10) ? null : reader.GetDateTime(10),
                OffrowCleanerEndTime = reader.IsDBNull(11) ? null : reader.GetDateTime(11),
                SkippedLowWaterMark = reader.IsDBNull(12) ? null : Convert.ToInt64(reader.GetValue(12)),
                SkippedMinUsefulXts = reader.IsDBNull(13) ? null : Convert.ToInt64(reader.GetValue(13)),
                SkippedOldestAborted = reader.IsDBNull(14) ? null : Convert.ToInt64(reader.GetValue(14))
            });
        }

        return items;
    }
}

/// <summary>
/// One database's ADR persistent version store state (#1951) — the byte-for-byte twin of Lite's
/// <c>PvsStatsRow</c>, including the derived members, because the two viewers' grids bind the same paths.
/// Every size is MEGABYTES converted from the DMV's kilobytes, and every PVS size is OFF-ROW only.
/// </summary>
public sealed class PvsStatsRow
{
    public string DatabaseName { get; set; } = "";
    public bool? IsAdrOn { get; set; }
    public decimal? PvsSizeMb { get; set; }
    public decimal? OnlineIndexVersionStoreMb { get; set; }
    public decimal? DatabaseDataSizeMb { get; set; }
    public long? AbortedTransactionCount { get; set; }
    public long? OldestActiveTransactionId { get; set; }
    public long? OldestAbortedTransactionId { get; set; }
    public DateTime? AbortedCleanerStartTime { get; set; }
    public DateTime? AbortedCleanerEndTime { get; set; }
    public DateTime? OffrowCleanerStartTime { get; set; }
    public DateTime? OffrowCleanerEndTime { get; set; }
    public long? SkippedLowWaterMark { get; set; }
    public long? SkippedMinUsefulXts { get; set; }
    public long? SkippedOldestAborted { get; set; }

    public string AdrDisplay => IsAdrOn switch
    {
        true => "On",
        false => "Off",
        null => "-"
    };

    /// <summary>
    /// PVS as a share of the database's online data files — the ratio MS's troubleshooting guide reads
    /// first ("PVS is considered large if it's significantly larger than the baseline or if it's close to
    /// 50% of the database size"). Guarded rather than divided blindly: MS's own published query divides
    /// by this denominator unguarded and would fail on a database with no online data files.
    /// </summary>
    public decimal? PvsPercentOfDatabase =>
        PvsSizeMb.HasValue && DatabaseDataSizeMb > 0
            ? Math.Round(PvsSizeMb.Value * 100m / DatabaseDataSizeMb.Value, 1)
            : null;

    /// <summary>
    /// How far the oldest ABORTED transaction lags the oldest ACTIVE one, in the DMV's own internal
    /// sequence numbers. This is the input to MS's documented read — "if the oldest_aborted_transaction_id
    /// is much lower than oldest_active_transaction_id, and the current_abort_transaction_count value is
    /// large, there's likely an old aborted transaction preventing PVS cleanup" — presented as the gap
    /// itself rather than as a yes/no verdict.
    ///
    /// <para>Deliberately NOT a boolean. "Much lower" and "large" have no documented thresholds, these are
    /// dense internal sequence numbers whose scale is instance- and workload-specific, and a flag reading
    /// "Likely" off an id one lower than the active one would fire constantly on benign state. Inventing a
    /// cutoff would be exactly the folklore this collector went out of its way to avoid when it dropped
    /// Microsoft's two non-resolving joins. The operator sees the gap, the aborted count, and the
    /// skipped-page counters, and makes the call MS asks them to make.</para>
    ///
    /// <para>Null unless BOTH ids are non-zero: zero is the DMV's "none tracked" sentinel, not a low value,
    /// so subtracting through it would manufacture a huge fake gap on an idle database.</para>
    /// </summary>
    public long? AbortedTransactionLag =>
        OldestAbortedTransactionId > 0 && OldestActiveTransactionId > 0
            ? OldestActiveTransactionId - OldestAbortedTransactionId
            : null;

    /// <summary>
    /// Cleanup state, from the pair of cleaner timestamps. MS: "If start time has value but the end time
    /// doesn't, it means PVS cleanup is ongoing on this database." Both cleaners are folded into one
    /// column because an operator asks "is cleanup running or stuck", not "which of the two cleaners".
    /// </summary>
    public string CleanupState
    {
        get
        {
            bool abortedRunning = AbortedCleanerStartTime.HasValue && !AbortedCleanerEndTime.HasValue;
            bool offrowRunning = OffrowCleanerStartTime.HasValue && !OffrowCleanerEndTime.HasValue;

            if (abortedRunning || offrowRunning)
            {
                return "Running";
            }

            if (AbortedCleanerEndTime.HasValue || OffrowCleanerEndTime.HasValue)
            {
                return "Idle";
            }

            return "Never run";
        }
    }

    /// <summary>Most recent completed cleanup across both cleaners; null until one has finished.</summary>
    public DateTime? LastCleanupEnd =>
        AbortedCleanerEndTime.HasValue && OffrowCleanerEndTime.HasValue
            ? (AbortedCleanerEndTime.Value > OffrowCleanerEndTime.Value ? AbortedCleanerEndTime : OffrowCleanerEndTime)
            : AbortedCleanerEndTime ?? OffrowCleanerEndTime;
}

/// <summary>One PVS trend point (#1984 stage 2): a database's off-row PVS size at one collection,
/// with the same %-of-database ratio the grid computes (null when the denominator was zero).</summary>
public sealed record PvsTrendPoint(string DatabaseName, DateTime CollectionTime, double PvsSizeMb, double? PctOfDatabase);
