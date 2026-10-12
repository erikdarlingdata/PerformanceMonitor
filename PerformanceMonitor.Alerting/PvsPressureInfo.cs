/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Alerting;

/// <summary>
/// One ADR database's latest persistent version store snapshot, as
/// <see cref="IAlertReadAdapter.GetPvsPressureAsync"/> returns it for the PVS-pressure alert
/// (#1984). Rows come only from databases with ADR ON — a database that cannot have a PVS cannot
/// breach — and only from the newest pvs_stats collection, matching the low-disk alert's
/// latest-snapshot convention. Threshold evaluation stays engine-side
/// (<see cref="AlertContextBuilders.GetBreachedPvsDatabases"/>).
/// </summary>
public class PvsPressureInfo
{
    public string DatabaseName { get; set; } = "";

    /// <summary>Off-row PVS size in MB — understates total version space by design; see PvsStatsCollector.</summary>
    public double PvsSizeMb { get; set; }

    /// <summary>Online data files (type 0) in MB — MS's own denominator, and it INCLUDES the PVS itself.</summary>
    public double DatabaseDataSizeMb { get; set; }

    public long CurrentAbortedTransactionCount { get; set; }

    public long OldestActiveTransactionId { get; set; }

    public long OldestAbortedTransactionId { get; set; }

    /// <summary>True when the aborted-version cleaner has a start time but no end time — MS documents
    /// that shape as "cleanup is ongoing on this database".</summary>
    public bool AbortedCleanupOngoing { get; set; }

    /// <summary>The instant this row's source data was last collected (UTC), or null when the host does not
    /// have a UTC instant for it (#5642). Feeds the alert's newest-sample age line
    /// (<c>AlertContext.NewestSample</c>). Read from the same <c>MAX(collection_time)</c> the check already
    /// reads; a pre-UTC local stamp is never put here, so null means "no age line", not "unknown age".</summary>
    public System.DateTime? NewestSampleUtc { get; set; }

    /// <summary>
    /// PVS as a share of the database's online data files — the ratio MS's troubleshooting guide
    /// reads first ("close to 50% of the database size" = large). Same guarded formula as both
    /// FinOps grids' PvsPercentOfDatabase; 0 when the denominator is missing rather than a divide
    /// error on a database with no online data files.
    /// </summary>
    public double PvsPercent => DatabaseDataSizeMb > 0 ? PvsSizeMb / DatabaseDataSizeMb * 100 : 0;

    public double PvsGb => PvsSizeMb / 1024.0;

    /// <summary>
    /// How far the oldest ABORTED transaction lags the oldest ACTIVE one, in the DMV's internal
    /// sequence numbers — the input to MS's "old aborted transaction is preventing cleanup" read.
    /// Deliberately NOT a verdict and never a trigger: "much lower" has no documented threshold
    /// (the same reasoning as the FinOps grids' AbortedTransactionLag). Null unless BOTH ids are
    /// non-zero — zero is the DMV's "none tracked" sentinel, and subtracting through it would
    /// manufacture a huge fake gap on an idle database.
    /// </summary>
    public long? AbortedTransactionLag =>
        OldestAbortedTransactionId > 0 && OldestActiveTransactionId > 0
            ? OldestActiveTransactionId - OldestAbortedTransactionId
            : null;
}
