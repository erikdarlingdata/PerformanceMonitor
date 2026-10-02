/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// Gets CPU utilization data for charting.
    /// Note: sample_time is stored in server local time (from SYSDATETIME()), not UTC, and in the default
    /// <see cref="CpuTimeFrame.ServerLocal"/> frame the projected <c>SampleTime</c> stays that way, with no offset
    /// applied. The WPF charts ask for <see cref="CpuTimeFrame.Utc"/> and plot <c>SampleTimeUtc</c>, the instant.
    ///
    /// <para><b>The WINDOW prefers the stored UTC instant (v63, #3653 item 13, Q7).</b> Since that rung the
    /// collector writes <c>sample_time_utc</c> — the same instant in UTC — beside the local stamp. This read's
    /// window is a UTC question (<paramref name="hoursBack"/> back from <paramref name="asOfUtc"/>, or a
    /// custom range the caller holds as UTC instants), and each row answers it on the stamp it has. A row
    /// with a <c>sample_time_utc</c> is compared on it, against the UTC bounds, with no offset involved. A row
    /// collected before that rung has none, and the offset the server had at that row's instant is exactly what
    /// the store never recorded, so it is compared on the only stamp it has: its server-local
    /// <c>sample_time</c> against the window's server-local bounds (<c>GetTimeRangeServerLocal</c>, each bound
    /// the server's clock at its own instant). The predicate is
    /// <c>(sample_time_utc IS NOT NULL AND sample_time_utc BETWEEN utcStart AND utcEnd) OR (sample_time_utc IS
    /// NULL AND sample_time BETWEEN localStart AND localEnd)</c>, so a window that spans a daylight saving
    /// change selects both kinds of row exactly on each side of it, where one offset for the whole window would
    /// put every pre-rung row on the other side of the change an hour off (#4766). Nothing is backfilled. Known
    /// limit (#4766): a pre-rung row stamped in the repeated autumn hour carries a wall time that names two
    /// instants, so a window with a bound inside that hour compares it on the wall time alone. The WPF chart
    /// and the Overview lane read this window; the MCP <c>get_cpu_utilization</c> reads the same window through
    /// <c>GetCpuBucketsAsync</c>, in the server-local frame, so both windows are honest for post-rung rows and
    /// the frame this read is asked for never reaches the MCP tool.</para>
    ///
    /// <para><b>The FRAME the points are bucketed and stamped in (#4766).</b> Every point carries both clocks,
    /// <see cref="CpuUtilizationRow.SampleTime"/> (the server's wall clock) and
    /// <see cref="CpuUtilizationRow.SampleTimeUtc"/> (the instant), and <paramref name="frame"/> says which one
    /// the buckets are cut on.
    /// <see cref="CpuTimeFrame.ServerLocal"/> (the default) buckets every row on its server-local
    /// <c>sample_time</c> exactly as this read always has, so the two readings of a repeated autumn hour (01:30
    /// EDT and 01:30 EST carry the same wall time) share a bucket and come back as ONE point, stamped
    /// <c>SampleTime</c> as before; its <c>SampleTimeUtc</c> is that stamp taken through the clock, which for a
    /// wall time that names two instants is the first. <see cref="CpuTimeFrame.Utc"/> buckets a row that has a
    /// <c>sample_time_utc</c> on it, so those two readings are TWO points, each at its own instant; a pre-rung
    /// row, which has only the wall time, is bucketed on it and its bucket start taken through the clock
    /// (never merged into a post-rung bucket, since the two are cut in different frames), and the points come
    /// back in instant order. In that frame <c>SampleTime</c> is the server's wall clock at
    /// <c>SampleTimeUtc</c>, so the two repeated-hour points share a <c>SampleTime</c> and differ in
    /// <c>SampleTimeUtc</c>. In both frames a window narrow enough that no bucket merged two collections
    /// stamps each point at its sample's own clock, not the grid line.</para>
    /// </summary>
    /// <param name="serverClock">
    /// <paramref name="serverId"/>'s OWN clock, which is what the server-local window has to be
    /// expressed in. <c>null</c> means "use the desktop UI's selected-tab clock"
    /// (<see cref="ServerTimeHelper.ActiveServerClock"/>) — correct for the WPF tab that set it and for
    /// nobody else. Any caller that picks its own <paramref name="serverId"/> (every MCP tool does) must
    /// pass that server's clock, or the window and the server_id name two different servers and the read
    /// comes back shifted or empty with no error.
    /// </param>
    /// <param name="frame">
    /// The clock the points are bucketed on (#4766); see the frame paragraph above. Defaults to
    /// <see cref="CpuTimeFrame.ServerLocal"/>, which every caller before this parameter existed gets unchanged.
    /// </param>
    public async Task<List<CpuUtilizationRow>> GetCpuUtilizationAsync(int serverId, int hoursBack = 4, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null, ServerClock? serverClock = null, CpuTimeFrame frame = CpuTimeFrame.ServerLocal)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        /* sample_time is in server local time, not UTC. Both windows are asked for: the UTC bounds, the same
           instants taken from the one anchor so a preset window is exact (a custom range arrives as UTC and
           passes through GetTimeRange unchanged, #4766), and the server-local bounds this read always computed. A row with a sample_time_utc is
           compared on it against the UTC bounds; a row without one (collected before v63) is compared on its
           server-local sample_time against the server-local bounds, so neither arm applies one offset to the
           whole window and both are exact on each side of a daylight saving change. */
        var clock = serverClock ?? ServerTimeHelper.ActiveServerClock;
        var anchor = asOfUtc ?? DateTime.UtcNow;
        var (startUtc, endUtc) = GetTimeRange(hoursBack, fromDate, toDate, anchor);
        var (startTime, endTime) = GetTimeRangeServerLocal(hoursBack, fromDate, toDate, anchor, clock);

        /* #4234: bucketed to TrendBudget.Chart's point budget so the Overview lane and this same read's CPU
           tab chart stop shipping one point per collection over a multi-day window. seriesCount is always 1 —
           SqlServerCpu/OtherProcessCpu ride the SAME row/bucket, not separate series. The bucket width comes
           from the window's real length, not its wall-clock span, which a daylight saving change moves by an hour. */
        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endUtc - startUtc).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        var utcFrame = frame == CpuTimeFrame.Utc;
        command.CommandText = CpuUtilizationTrendSqlFor(frame);

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startUtc });
        command.Parameters.Add(new DuckDBParameter { Value = endUtc });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = bucketMinutes });

        /* BucketIsServerLocal says which clock BucketStart and FirstSampleTime are in. The server-local frame cuts
           every bucket on sample_time, so it is always true and the statement does not return it; the UTC frame
           returns it as a sixth column, false for a bucket of rows that had a sample_time_utc. */
        var rows = new List<(DateTime BucketStart, int SqlCpu, int OtherCpu, DateTime FirstSampleTime, long SampleCount, bool BucketIsServerLocal)>();
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
                sampleCount,
                !utcFrame || reader.GetBoolean(5)));
        }

        /* Every bucket in this call held exactly one physical sample: stamp each point at that sample's OWN
           clock instead of the bucket grid line, so a window narrow enough to never merge two collections
           renders byte-identical to the pre-#4234 per-collection read. */
        var items = new List<CpuUtilizationRow>(rows.Count);
        var serverZone = clock.AsTimeZone();
        foreach (var row in rows)
        {
            var stamp = everyBucketSingleton ? row.FirstSampleTime : row.BucketStart;
            var item = new CpuUtilizationRow
            {
                SqlServerCpu = row.SqlCpu,
                OtherProcessCpu = row.OtherCpu
            };

            if (utcFrame)
            {
                /* A bucket of post-rung rows was cut on the instant already; one of pre-rung rows was cut on the
                   wall time, and the clock says which instant that names. The wall clock shown beside it is the
                   server's at that instant, so two readings of a repeated hour read the same wall time. The
                   point says when that wall time names two instants, because a chart can only word such a point
                   as a wall time; a wall time that happens once converted exactly and needs no such word. */
                var instant = row.BucketIsServerLocal ? clock.ToUtc(stamp) : stamp;
                item.SampleTimeUtc = instant;
                item.SampleTime = clock.ToServerLocal(instant);
                item.SampleTimeNamesTwoInstants = row.BucketIsServerLocal
                    && serverZone.IsAmbiguousTime(DateTime.SpecifyKind(stamp, DateTimeKind.Unspecified));
            }
            else
            {
                item.SampleTime = stamp;
                item.SampleTimeUtc = clock.ToUtc(stamp);
                item.SampleTimeNamesTwoInstants = serverZone.IsAmbiguousTime(DateTime.SpecifyKind(stamp, DateTimeKind.Unspecified));
            }

            items.Add(item);
        }

        /* The statement can only order a bucket within the clock it was cut on, so the pre-rung buckets come
           back apart from the post-rung ones; the stable sort puts them in one line by instant. */
        return utcFrame ? items.OrderBy(i => i.SampleTimeUtc).ToList() : items;
    }

    /// <summary>
    /// The bucketed CPU trend statement text (#4234) in the server-local frame,
    /// <see cref="CpuUtilizationTrendSqlFor"/>'s <see cref="CpuTimeFrame.ServerLocal"/> statement.
    /// </summary>
    internal static string CpuUtilizationTrendSql => CpuUtilizationTrendSqlFor(CpuTimeFrame.ServerLocal);

    /// <summary>
    /// The bucketed CPU trend statement text (#4234), pulled out of <see cref="GetCpuUtilizationAsync"/> so its
    /// shape is checkable without a live DuckDB. $1 server_id, $2/$3 the UTC window (the bounds for a row that
    /// has a <c>sample_time_utc</c>), $4/$5 the server-local window (the bounds for a pre-v63 row with none;
    /// $4 also clamps <c>time_bucket</c>'s grid line so the first bucket never renders earlier than the window
    /// the caller asked for), $6 the bucket width in minutes. A NULL reading counts as 0 in the average,
    /// exactly as the per-collection read always counted it in C#; <see cref="TrendBuckets.OriginSql"/> is the
    /// same origin every bucketed trend in this app aligns to, so a width that does not divide a day still
    /// bins consistently.
    ///
    /// <para>One statement, two frames (#4766); the frame changes only the bucket expression, the first-sample
    /// column, the grouping and one extra column, never the WHERE, so both frames select the same rows.
    /// <see cref="CpuTimeFrame.ServerLocal"/> is the statement as it has always been: every row bucketed on
    /// <c>sample_time</c>, five columns (<c>bucket_start</c>, <c>sql_server_cpu</c>, <c>other_process_cpu</c>,
    /// <c>first_sample_time</c>, <c>sample_count</c>), all in the server's clock.
    /// <see cref="CpuTimeFrame.Utc"/> buckets a row that has a <c>sample_time_utc</c> on it, its grid line
    /// clamped to $2 (the UTC start), and a row that has none on <c>sample_time</c>, its grid line clamped to
    /// $4 (the server-local start), and groups on that choice as well as the bucket so the two kinds never
    /// share one. It returns the choice as a sixth column, <c>bucket_is_server_local</c>, because
    /// <c>bucket_start</c> and <c>first_sample_time</c> are in UTC where it is false and in the server's clock
    /// where it is true. A window over the moment the collector was upgraded can therefore split one bucket
    /// into two, one of each kind.</para>
    /// </summary>
    internal static string CpuUtilizationTrendSqlFor(CpuTimeFrame frame)
    {
        var utc = frame == CpuTimeFrame.Utc;

        var serverLocalBucket = $"GREATEST(time_bucket(to_minutes(CAST($6 AS INTEGER)), sample_time, {TrendBuckets.OriginSql}), $4)";
        var utcBucket = $"GREATEST(time_bucket(to_minutes(CAST($6 AS INTEGER)), sample_time_utc, {TrendBuckets.OriginSql}), $2)";

        /* The fragments are inline (no line breaks) so the server-local statement keeps its exact text. */
        var rawUtcColumn = utc ? " sample_time_utc," : string.Empty;
        var bucket = utc
            ? $"CASE WHEN sample_time_utc IS NULL THEN {serverLocalBucket} ELSE {utcBucket} END"
            : serverLocalBucket;
        var firstSample = utc ? "MIN(COALESCE(sample_time_utc, sample_time))" : "MIN(sample_time)";
        var bucketIsServerLocal = utc ? ", (sample_time_utc IS NULL) AS bucket_is_server_local" : string.Empty;
        var groupBy = utc ? "1, 6" : "1";

        return $@"
WITH raw AS
(
    SELECT
        sample_time,{rawUtcColumn}
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
    {bucket} AS bucket_start,
    CAST(ROUND(AVG(COALESCE(sqlserver_cpu_utilization, 0))) AS INTEGER) AS sql_server_cpu,
    CAST(ROUND(AVG(COALESCE(other_process_cpu_utilization, 0))) AS INTEGER) AS other_process_cpu,
    {firstSample} AS first_sample_time,
    COUNT(*) AS sample_count{bucketIsServerLocal}
FROM raw
GROUP BY {groupBy}
ORDER BY {groupBy}";
    }

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

/// <summary>
/// The clock <see cref="LocalDataService.GetCpuUtilizationAsync"/> cuts its buckets on (#4766), and so what a
/// point's two stamps mean. Every point carries both <see cref="CpuUtilizationRow.SampleTime"/> and
/// <see cref="CpuUtilizationRow.SampleTimeUtc"/> in either frame; the frame decides which of the two is the
/// one the bucket was cut on.
/// </summary>
public enum CpuTimeFrame
{
    /// <summary>
    /// The server's wall clock, and the default: every row is bucketed on its server-local <c>sample_time</c>
    /// and the point is stamped on it, as this read always has. The two readings of a repeated autumn hour
    /// carry the same wall time, so they share a bucket and are one point; that point's
    /// <see cref="CpuUtilizationRow.SampleTimeUtc"/> is the first of the two instants its wall time names.
    /// </summary>
    ServerLocal = 0,

    /// <summary>
    /// The instant: a row that has a <c>sample_time_utc</c> is bucketed on it, so the two readings of a
    /// repeated hour are two points, each at its own <see cref="CpuUtilizationRow.SampleTimeUtc"/>. A row
    /// collected before that column existed is bucketed on its wall time and its bucket start is taken
    /// through the server's clock (the first instant, where the wall time names two), and a point whose wall time
    /// does name two says so in <see cref="CpuUtilizationRow.SampleTimeNamesTwoInstants"/>. The points are in
    /// instant order, and <see cref="CpuUtilizationRow.SampleTime"/> is the server's wall clock at
    /// <see cref="CpuUtilizationRow.SampleTimeUtc"/>.
    /// </summary>
    Utc = 1,
}

/// <summary>
/// One CPU point: a bucket's averages, or a single collection's own reading when no bucket merged two. The frame
/// the read was asked for (<see cref="CpuTimeFrame"/>) says how the point was cut; the two stamps are always set.
/// </summary>
public class CpuUtilizationRow
{
    /// <summary>
    /// The point's stamp on the server's wall clock. In the <see cref="CpuTimeFrame.ServerLocal"/> frame this is
    /// the stamp the point was cut on, so two readings of a repeated hour are one point at one
    /// <c>SampleTime</c>; in the <see cref="CpuTimeFrame.Utc"/> frame it is the server's clock at
    /// <see cref="SampleTimeUtc"/>, so they are two points that share it.
    /// </summary>
    public DateTime SampleTime { get; set; }

    /// <summary>
    /// The point's instant in UTC, naive (Kind Unspecified). In the <see cref="CpuTimeFrame.Utc"/> frame it is the
    /// bucket the point was cut on (its start, or the collection's own instant when no bucket merged two); in the
    /// <see cref="CpuTimeFrame.ServerLocal"/> frame it is <see cref="SampleTime"/> taken through the server's
    /// clock, and never unset.
    /// </summary>
    public DateTime SampleTimeUtc { get; set; }

    /// <summary>
    /// True when the bucket was cut on the server's stored wall clock AND that wall time happens twice there (#4766):
    /// the repeated hour of the autumn change. <see cref="SampleTimeUtc"/> is then only the first of the two instants
    /// the wall time names, so it cannot say which pass of the repeated hour the bucket was. That covers a point in
    /// the <see cref="CpuTimeFrame.ServerLocal"/> frame, and in the <see cref="CpuTimeFrame.Utc"/> frame a bucket of
    /// rows collected before the UTC column existed, whose wall time falls in that hour. A stored wall time that
    /// happens once converts exactly, so it is false there, and false for a bucket cut on the stored instant. A
    /// server on a fixed offset, or on UTC, has no repeated hour, so it is false on every point.
    /// </summary>
    public bool SampleTimeNamesTwoInstants { get; set; }

    public int SqlServerCpu { get; set; }
    public int OtherProcessCpu { get; set; }
    public int TotalCpu => SqlServerCpu + OtherProcessCpu;
    public int IdleCpu => Math.Max(0, 100 - TotalCpu);
}
