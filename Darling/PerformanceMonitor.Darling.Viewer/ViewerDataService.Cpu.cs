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

/// <summary>
/// One RAW CPU ring-buffer sample: a sample time plus the SQL Server and other-process CPU percentages.
/// This raw-per-sample read (up to ~60 samples per collection) feeds BOTH the CPU tab's scatter chart
/// (SQL Server vs other processes) and — since W1d — the Overview's CPU lane (SQL vs SQL+other Total),
/// mirroring Lite's CPU tab (<c>ServerTab.Charts.cs</c> <c>UpdateCpuChart</c>) and Lite's Overview lanes,
/// both over <c>LocalDataService.GetCpuUtilizationAsync</c>. Both CPU columns are <c>integer</c> in the
/// store, read as int and cast to double at plot time, byte-for-byte with Lite's chart body.
/// <c>SampleTime</c> is the store's server-LOCAL <c>sample_time</c> de-skewed to naive UTC by the read
/// (see <see cref="CpuUtilizationSql"/>, #1262), so both consumers plot it through
/// <see cref="ViewerTimeHelper.ForDisplay"/> like every other Darling series.
/// </summary>
public sealed record CpuUtilizationSample(DateTime SampleTime, int SqlServerCpu, int OtherProcessCpu);

public sealed partial class ViewerDataService
{
    /// <summary>
    /// The raw per-sample CPU read: every ring-buffer sample since <paramref name="sinceUtc"/> (not an
    /// average-per-collection roll-up), feeding both the CPU tab's scatter chart and — since W1d — the
    /// Overview's CPU lane. The window filters on <c>collection_time</c> (the naive-UTC collection
    /// prefix — the reliable clock every Darling read windows on); a NULL
    /// <c>other_process_cpu_utilization</c> (SQL on Linux, #1048) reads as 0.
    ///
    /// <para><b>sample_time de-skew (#1262).</b> Unlike every other stored column, <c>sample_time</c> is
    /// the MONITORED SERVER'S LOCAL wall clock (<c>SYSDATETIME()</c> on the server, minus each
    /// ring-buffer sample's age), NOT naive UTC — so feeding it straight to <see cref="ViewerTimeHelper.ForDisplay"/>
    /// (which assumes naive-UTC input) shifts the whole CPU series by the server's UTC offset relative
    /// to every <c>collection_time</c>-based lane — a visible misalignment in the correlated Overview
    /// (e.g. a Pacific-time server, offset -7/-8h, sits 7-8h off).
    /// The viewer has no per-server timezone config the way Lite does
    /// (<c>ServerTimeHelper.UtcOffsetMinutes</c>), so we recover the offset from the batch itself: all
    /// rows of one collection share a single <c>collection_time</c> (the batch key — <c>collection_id</c>
    /// is assigned per row, a distinct value per sample, NOT a batch id), and the NEWEST sample in a
    /// batch is at most ~1 minute older than that collection instant. So
    /// <c>MAX(sample_time) OVER (batch) - collection_time</c> is
    /// the server's UTC offset plus that sub-minute anchor age; rounding to 15 minutes recovers the exact
    /// whole/half/quarter-hour offset and absorbs the anchor age (and any few-second clock skew).
    /// Subtracting that per-batch offset turns each local <c>sample_time</c> into true naive UTC, so
    /// <see cref="ViewerTimeHelper.ForDisplay"/> then aligns the CPU series with every other lane. A UTC server yields
    /// offset 0 — byte-identical to the pre-fix read. The per-batch derivation also handles a DST
    /// transition inside the window (each batch recovers its own offset). Reads the base
    /// <c>cpu_utilization_stats</c> table. $1 server_id, $2 window start (naive UTC).</para>
    /// </summary>
    public const string CpuUtilizationSql = """
        SELECT
            /* De-skew sample_time (the monitored server's LOCAL wall clock) to naive UTC by subtracting
               the per-batch UTC offset = round(MAX(sample_time) over the batch - collection_time) to the
               nearest 15 minutes. collection_time (shared by every row of a collection) is the batch key;
               collection_id is assigned per row (a distinct value per sample). A UTC server yields offset
               0. See the C# summary above for the full derivation (#1262). */
            sample_time
                - INTERVAL '15 minutes'
                  * ROUND(EXTRACT(EPOCH FROM (
                        MAX(sample_time) OVER (PARTITION BY server_id, collection_time) - collection_time
                    )) / 900.0)::double precision AS sample_time,
            sqlserver_cpu_utilization,
            other_process_cpu_utilization
        FROM cpu_utilization_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        ORDER BY sample_time
        """;

    /// <summary>
    /// Raw CPU samples for one server since <paramref name="sinceUtc"/>, time-ordered — one point per
    /// ring-buffer sample, feeding both the CPU tab's scatter chart and the Overview's CPU lane. Each
    /// sample's <c>SampleTime</c> is de-skewed to naive UTC in the read (see <see cref="CpuUtilizationSql"/>,
    /// #1262), so both consumers can plot it through <see cref="ViewerTimeHelper.ForDisplay"/> unchanged.
    /// </summary>
    public async Task<List<CpuUtilizationSample>> GetCpuUtilizationAsync(int serverId, DateTime sinceUtc, CancellationToken cancellationToken = default)
    {
        var samples = new List<CpuUtilizationSample>();

        await using var command = _dataSource.CreateCommand(CpuUtilizationSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(sinceUtc, DateTimeKind.Unspecified),
        });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            samples.Add(new CpuUtilizationSample(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                /* NULL other-process CPU (SQL on Linux) reads as 0, like Lite's CPU chart. */
                reader.IsDBNull(2) ? 0 : reader.GetInt32(2)));
        }

        return samples;
    }
}
