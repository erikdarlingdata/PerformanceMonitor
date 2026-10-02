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
using PerformanceMonitor.Alerting;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// The newest pvs_stats snapshot's ADR databases for the PVS-pressure alert (#1984) — the alert-read
    /// twin of <see cref="GetPvsStatsLatestAsync"/>, narrowed the way the alert needs it: ADR-ON rows only
    /// (a database that cannot have a PVS cannot breach), worst (highest PVS share) first, mapped to the
    /// shared engine's <see cref="PvsPressureInfo"/>. Threshold evaluation stays engine-side.
    /// </summary>
    public async Task<List<PvsPressureInfo>> GetPvsPressureAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT
    database_name,
    persistent_version_store_size_mb,
    database_data_size_mb,
    current_aborted_transaction_count,
    oldest_active_transaction_id,
    oldest_aborted_transaction_id,
    aborted_version_cleaner_start_time,
    aborted_version_cleaner_end_time
FROM v_pvs_stats
WHERE server_id = $1
AND   collection_time = (
    SELECT MAX(collection_time)
    FROM v_pvs_stats
    WHERE server_id = $1
)
AND   is_accelerated_database_recovery_on
ORDER BY
    CASE WHEN database_data_size_mb > 0
         THEN persistent_version_store_size_mb / database_data_size_mb
         ELSE 0 END DESC";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        var items = new List<PvsPressureInfo>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new PvsPressureInfo
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                PvsSizeMb = reader.IsDBNull(1) ? 0 : Convert.ToDouble(reader.GetValue(1)),
                DatabaseDataSizeMb = reader.IsDBNull(2) ? 0 : Convert.ToDouble(reader.GetValue(2)),
                CurrentAbortedTransactionCount = reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                OldestActiveTransactionId = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                OldestAbortedTransactionId = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                /* MS's documented shape for "cleanup is ongoing": a start time with no end time. */
                AbortedCleanupOngoing = !reader.IsDBNull(6) && reader.IsDBNull(7)
            });
        }

        return items;
    }

    /// <summary>
    /// #1984 stage 2: the PVS trend behind the FinOps chart — every stored point over the window for
    /// the TOP-5 databases by PVS size at the newest collection. Percent-of-database is computed per
    /// POINT from the same row's data-file denominator, the exact ratio the grid shows, so the two
    /// surfaces cannot disagree. Mirrored by the Darling viewer's <c>GetPvsTrendAsync</c> — same
    /// columns, same top-N pin, same ordering — so the twins cannot drift.
    /// </summary>
    public async Task<List<PvsTrendPoint>> GetPvsTrendAsync(int serverId, DateTime sinceUtc)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
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
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(sinceUtc, DateTimeKind.Unspecified) });

        var items = new List<PvsTrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            /* #3653: a NULL size is an UNMEASURED point (the collector could not read the version store that
               pass), not an empty one — the same class as the trend rates' fabricated first-point 0 (#3642).
               It is carried as null: the get_pvs_trend payload publishes it as such (a collection happened; its
               size is unknown — contract rule 5, zero is a measurement), and the FinOps chart leaves it out
               rather than drawing a cliff into a series that has none. The Darling viewer's reader, which has
               no payload consumer, skips the row outright. */
            items.Add(new PvsTrendPoint(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.GetDateTime(1),
                reader.IsDBNull(2) ? null : ToDouble(reader.GetValue(2)),
                reader.IsDBNull(3) ? null : ToDouble(reader.GetValue(3))));
        }

        return items;
    }

    /// <summary>
    /// Latest ADR persistent version store snapshot, one row per database (#1951). Reads the archive view
    /// so a window that has aged into parquet still resolves, and pins to the newest collection_time the
    /// way the sibling database-size read does — this grid answers "what is my version store doing right
    /// now", not "how did it get here".
    /// </summary>
    public async Task<List<PvsStatsRow>> GetPvsStatsLatestAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
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
    pvs_off_row_page_skipped_oldest_aborted_xdesid,
    collection_time
FROM v_pvs_stats
WHERE server_id = $1
AND   collection_time = (
    SELECT MAX(collection_time)
    FROM v_pvs_stats
    WHERE server_id = $1
)
ORDER BY persistent_version_store_size_mb DESC NULLS LAST, database_name";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        var items = new List<PvsStatsRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new PvsStatsRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                IsAdrOn = reader.IsDBNull(1) ? null : (bool?)(Convert.ToInt32(reader.GetValue(1)) == 1),
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
                SkippedOldestAborted = reader.IsDBNull(14) ? null : Convert.ToInt64(reader.GetValue(14)),
                CollectionTime = reader.GetDateTime(15)
            });
        }

        return items;
    }
}

/// <summary>
/// One database's ADR persistent version store state (#1951). Every size is MEGABYTES converted from the
/// DMV's kilobytes, and every PVS size is OFF-ROW only — MS documents
/// <c>persistent_version_store_size_kb</c> as excluding versions stored in-row, so a database can read a
/// small PVS and still carry version overhead. The grid header says so.
/// </summary>
public class PvsStatsRow
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
    public DateTime CollectionTime { get; set; }

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
/// with the same %-of-database ratio the grid computes (null when the denominator was zero).
/// <see cref="PvsSizeMb"/> is null for an UNMEASURED collection (#3653) — the DMV reported no size that
/// pass — never coerced to 0.</summary>
public sealed record PvsTrendPoint(string DatabaseName, DateTime CollectionTime, double? PvsSizeMb, double? PctOfDatabase);
