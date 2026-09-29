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
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// Gets CPU utilization data for charting.
    /// Note: sample_time is stored in server local time (from SYSDATETIME()), not UTC, and the projected
    /// <c>SampleTime</c> stays that way — the chart plots the server's own frame, and the local stamp IS that
    /// frame with no offset applied.
    ///
    /// <para><b>The WINDOW prefers the stored UTC instant (v63, #3653 item 13, Q7).</b> Since that rung the
    /// collector writes <c>sample_time_utc</c> — the same instant in UTC — beside the local stamp. This read's
    /// window is a UTC question (<paramref name="hoursBack"/> back from <paramref name="asOfUtc"/>, or a
    /// picker range the caller expressed in server time), and each row answers it on the stamp it has. A row
    /// with a <c>sample_time_utc</c> is compared on it, against the UTC bounds, with no offset involved. A row
    /// collected before that rung has none, and the offset the server had at that row's instant is exactly what
    /// the store never recorded, so it is compared on the only stamp it has: its server-local
    /// <c>sample_time</c> against the window's server-local bounds (<c>GetTimeRangeServerLocal</c>, each bound
    /// the server's clock at its own instant). The predicate is
    /// <c>(sample_time_utc IS NOT NULL AND sample_time_utc BETWEEN utcStart AND utcEnd) OR (sample_time_utc IS
    /// NULL AND sample_time BETWEEN localStart AND localEnd)</c>, so a window that spans a daylight saving
    /// change selects both kinds of row exactly on each side of it, where one offset for the whole window would
    /// put every pre-rung row on the other side of the change an hour off (#4766). Nothing is backfilled. Known
    /// limit (#4766): in the repeated autumn hour a server-local time resolves to its first occurrence. The MCP
    /// <c>get_cpu_utilization</c> and the WPF chart both come through here, so both windows are honest for
    /// post-rung rows and neither's display frame changes.</para>
    /// </summary>
    /// <param name="serverClock">
    /// <paramref name="serverId"/>'s OWN clock, which is what the server-local window has to be
    /// expressed in. <c>null</c> means "use the desktop UI's selected-tab clock"
    /// (<see cref="ServerTimeHelper.ActiveServerClock"/>) — correct for the WPF tab that set it and for
    /// nobody else. Any caller that picks its own <paramref name="serverId"/> (every MCP tool does) must
    /// pass that server's clock, or the window and the server_id name two different servers and the read
    /// comes back shifted or empty with no error.
    /// </param>
    public async Task<List<CpuUtilizationRow>> GetCpuUtilizationAsync(int serverId, int hoursBack = 4, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null, ServerClock? serverClock = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        /* sample_time is in server local time, not UTC. Both windows are asked for: the UTC bounds, the same
           instants taken from the one anchor so a preset window is exact (the picker branch converts the
           server-time range back to UTC exactly as GetTimeRange does, each bound with the offset in force
           there, #4766), and the server-local bounds this read always computed. A row with a sample_time_utc is
           compared on it against the UTC bounds; a row without one (collected before v63) is compared on its
           server-local sample_time against the server-local bounds, so neither arm applies one offset to the
           whole window and both are exact on each side of a daylight saving change. */
        var clock = serverClock ?? ServerTimeHelper.ActiveServerClock;
        var anchor = asOfUtc ?? DateTime.UtcNow;
        var (startUtc, endUtc) = GetTimeRange(hoursBack, fromDate, toDate, anchor, clock);
        var (startTime, endTime) = GetTimeRangeServerLocal(hoursBack, fromDate, toDate, anchor, clock);

        /* #4234: bucketed to TrendBudget.Chart's point budget so the Overview lane and this same read's CPU
           tab chart stop shipping one point per collection over a multi-day window. seriesCount is always 1 —
           SqlServerCpu/OtherProcessCpu ride the SAME row/bucket, not separate series. The bucket width comes
           from the window's real length, not its wall-clock span, which a daylight saving change moves by an hour. */
        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endUtc - startUtc).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        command.CommandText = CpuUtilizationTrendSql;

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startUtc });
        command.Parameters.Add(new DuckDBParameter { Value = endUtc });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = bucketMinutes });

        var rows = new List<(DateTime BucketStart, int SqlCpu, int OtherCpu, DateTime FirstSampleTime, long SampleCount)>();
        var everyBucketSingleton = true;

        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var sampleCount = reader.GetInt64(4);
            if (sampleCount != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.GetDateTime(0),
                reader.GetInt32(1),
                reader.GetInt32(2),
                reader.GetDateTime(3),
                sampleCount));
        }

        /* Every bucket in this call held exactly one physical sample: stamp each point at that sample's OWN
           clock instead of the bucket grid line, so a window narrow enough to never merge two collections
           renders byte-identical to the pre-#4234 per-collection read. */
        var items = new List<CpuUtilizationRow>(rows.Count);
        foreach (var row in rows)
        {
            items.Add(new CpuUtilizationRow
            {
                SampleTime = everyBucketSingleton ? row.FirstSampleTime : row.BucketStart,
                SqlServerCpu = row.SqlCpu,
                OtherProcessCpu = row.OtherCpu
            });
        }

        return items;
    }

    /// <summary>
    /// The bucketed CPU trend statement text (#4234), pulled out of <see cref="GetCpuUtilizationAsync"/> so its
    /// shape is checkable without a live DuckDB. $1 server_id, $2/$3 the UTC window (the bounds for a row that
    /// has a <c>sample_time_utc</c>), $4/$5 the server-local window (the bounds for a pre-v63 row with none;
    /// $4 also clamps <c>time_bucket</c>'s grid line so the first bucket never renders earlier than the window
    /// the caller asked for), $6 the bucket width in minutes. A NULL reading counts as 0 in the average,
    /// exactly as the per-collection read always counted it in C#; <see cref="TrendBuckets.OriginSql"/> is the
    /// same origin every bucketed trend in this app aligns to, so a width that does not divide a day still
    /// bins consistently.
    /// </summary>
    internal static string CpuUtilizationTrendSql => $@"
WITH raw AS
(
    SELECT
        sample_time,
        sqlserver_cpu_utilization,
        other_process_cpu_utilization
    FROM v_cpu_utilization_stats
    WHERE server_id = $1
    AND   (
              (sample_time_utc IS NOT NULL AND sample_time_utc >= $2 AND sample_time_utc <= $3)
           OR (sample_time_utc IS NULL AND sample_time >= $4 AND sample_time <= $5)
          )
)
SELECT
    GREATEST(time_bucket(to_minutes(CAST($6 AS INTEGER)), sample_time, {TrendBuckets.OriginSql}), $4) AS bucket_start,
    CAST(ROUND(AVG(COALESCE(sqlserver_cpu_utilization, 0))) AS INTEGER) AS sql_server_cpu,
    CAST(ROUND(AVG(COALESCE(other_process_cpu_utilization, 0))) AS INTEGER) AS other_process_cpu,
    MIN(sample_time) AS first_sample_time,
    COUNT(*) AS sample_count
FROM raw
GROUP BY 1
ORDER BY 1";

    /// <summary>
    /// The attributed-CPU denominator's pieces (#2320): sample count, coverage bounds, and average SQL
    /// CPU% over the window. Windowed on collection_time (UTC) — the SAME bounds the top-queries and
    /// top-procedures rankings use — so numerator and denominator share collection gaps; sample_time's
    /// server-local skew is irrelevant to an average. Takes the window EXPLICITLY (not hours_back) so
    /// the caller can hand the identical bounds to CpuAttribution.Compute — review catch: three
    /// independently-sampled UtcNow calls backing one disclosure is drift by construction.
    /// </summary>
    public async Task<CpuWindowAggregateRow> GetCpuWindowAggregateAsync(int serverId, DateTime startUtc, DateTime endUtc)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
SELECT
    COUNT(*),
    MIN(collection_time),
    MAX(collection_time),
    AVG(CAST(sqlserver_cpu_utilization AS DOUBLE))
FROM v_cpu_utilization_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startUtc });
        command.Parameters.Add(new DuckDBParameter { Value = endUtc });

        using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            return new CpuWindowAggregateRow(0, null, null, null);
        }

        return new CpuWindowAggregateRow(
            reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0)),
            reader.IsDBNull(1) ? null : reader.GetDateTime(1),
            reader.IsDBNull(2) ? null : reader.GetDateTime(2),
            reader.IsDBNull(3) ? null : reader.GetDouble(3));
    }
}

public sealed record CpuWindowAggregateRow(int SampleCount, DateTime? FirstSample, DateTime? LastSample, double? AvgSqlCpuPercent);

public class CpuUtilizationRow
{
    public DateTime SampleTime { get; set; }
    public int SqlServerCpu { get; set; }
    public int OtherProcessCpu { get; set; }
    public int TotalCpu => SqlServerCpu + OtherProcessCpu;
    public int IdleCpu => Math.Max(0, 100 - TotalCpu);
}
