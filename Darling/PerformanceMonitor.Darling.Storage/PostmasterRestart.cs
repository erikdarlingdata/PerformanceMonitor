/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Whether the interval between two samples of a PostgreSQL server's cumulative checkpointer counters SPANS a
/// postmaster restart (#3955). One rule for every reader that differences those counters: the store's own
/// checkpointer interval (<c>DarlingStoreMetricsReader.CheckpointerReading</c>, which the Store Checkpointer
/// Pressure self-alert judges) and the monitored-target write reads (<c>PgTargetFactCollector.PgTargetCheckpointSql</c>
/// and <see cref="DarlingPgWriteStatsReader.PgWriteStatsSql"/>). Written once in C# (<see cref="Spans"/>) and once
/// in SQL (<see cref="SpansSql"/>), and the two are held to one truth table by test.
///
/// <para><b>Why a restart makes the checkpoint figures unknown.</b> PostgreSQL counts a SHUTDOWN checkpoint in
/// <c>pg_stat_checkpointer.num_requested</c> (<c>pg_stat_bgwriter.checkpoints_req</c> through 16), and a clean
/// shutdown writes the statistics out, so the count survives the restart. Measured on the bundled 18.6: a fresh
/// cluster read <c>num_timed | num_requested | num_done</c> as 0|0|0, then 0|1|0 after one fast stop and start and
/// 0|2|0 after a second, with nothing in the server log but <c>checkpoint starting: shutdown immediate</c>; 17.10
/// read the same. A crash adds one the other way: an immediate stop discards the statistics and the
/// end-of-recovery checkpoint is counted as requested on the fresh counters. The shutdown checkpoint's own work
/// lands in the same counters as well: its write and sync phases (measured: 45 ms and 47 ms landed in
/// <c>write_time</c> and <c>sync_time</c> with that checkpoint's 5,029 buffers) and the buffers it flushed. A fast
/// shutdown flushes every dirty buffer at once with every client already gone, so on a large, slow store that
/// work can be seconds of fsync no reader sat inside. Nothing separates any of it from the live checkpoints' work,
/// so an interval that spans a restart has an UNKNOWN requested count, write time, sync time and checkpoint buffer
/// count: never zero, and never evidence of WAL or I/O pressure. The readers state none of them and judge neither
/// arm of any condition over them. <c>num_timed</c> (and <c>num_done</c>, 18+) does not move on a restart.</para>
///
/// <para><b>The cost</b> is one skipped interval per restart: the store's own hourly self-metrics interval, or one
/// collection interval of a monitored target's series. The next interval is judged normally. A read that
/// differences a whole window's edges rather than each interval (<see cref="DarlingPgWriteStatsReader"/>) withholds
/// those figures for any window that holds a restart, and states them again for a window that starts after
/// it.</para>
///
/// <para><b>The rule.</b> Every sample carries <c>pg_postmaster_start_time()</c> beside its counters, as naive UTC
/// in the <c>postmaster_start_time</c> column V139 added to <c>collect.store_metrics</c> (the <c>checkpointer</c>
/// row) and to <c>collect.pg_write_stats</c>.
/// <list type="bullet">
/// <item><b>Both samples carry it:</b> the interval spans a restart exactly when the two differ. That compares one
/// clock, the server's, with itself, so no skew between the host that stamps <c>metric_time</c> or
/// <c>collection_time</c> and the server can move the answer.</item>
/// <item><b>Only the newer carries it</b> (the older was written before V139; on a managed store that is the first
/// interval after the upgrade, which IS a restart): it spans one when the older sample was taken before the
/// newer sample's postmaster started. This arm compares the collecting host's clock with the server's. On a
/// managed store they are one machine; elsewhere a skew larger than the gap between the older sample and the
/// restart could misjudge this one interval, once per server.</item>
/// <item><b>The newer does not carry it</b> (written before V139, or by an older build after a downgrade): no
/// evidence either way. Unknown is not different, the <c>ServerEpoch.IsNewEpoch</c> discipline, so the interval
/// is judged exactly as it was before this rule existed.</item>
/// </list></para>
/// </summary>
public static class PostmasterRestart
{
    /// <summary>
    /// The rule in C#. <paramref name="previousSampleTime"/> is the older sample's stamp (naive UTC, the collecting
    /// host's clock); <paramref name="previousStart"/> and <paramref name="newestStart"/> are the two samples'
    /// <c>pg_postmaster_start_time()</c> (naive UTC, the server's clock), each null where the sample predates the
    /// column. <see cref="DateTime"/> equality and ordering compare ticks and ignore <see cref="DateTime.Kind"/>,
    /// so a caller that has already marked the stamps UTC gets the same answer.
    /// </summary>
    public static bool Spans(DateTime previousSampleTime, DateTime? previousStart, DateTime? newestStart)
    {
        if (newestStart is not DateTime newest)
        {
            return false;
        }

        return previousStart is DateTime previous
            ? previous != newest
            : previousSampleTime < newest;
    }

    /// <summary>
    /// The rule in SQL: one boolean expression over the CURRENT row and the row before it, for a read that
    /// differences a series under <c>WINDOW series AS (ORDER BY collection_time)</c>, the name both monitored-target
    /// reads give their window. It is never NULL. The first row of a window has no predecessor, so it compares NULL
    /// and is coalesced to false: the interval that ends there started outside the window, and no figure of the
    /// window contains it.
    /// </summary>
    public const string SpansSql =
        "coalesce(CASE WHEN postmaster_start_time IS NOT NULL AND LAG(postmaster_start_time) OVER series IS NOT NULL "
        + "THEN postmaster_start_time <> LAG(postmaster_start_time) OVER series "
        + "ELSE LAG(collection_time) OVER series < postmaster_start_time END, false)";
}
