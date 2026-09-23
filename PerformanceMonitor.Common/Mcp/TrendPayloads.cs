/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;

namespace PerformanceMonitor.Common;

/// <summary>One file I/O series the window saw activity on, in rank order (#3897): a (database, file type)
/// pair, or one file when the read is scoped to a database (<see cref="FileName"/> is null in the first grain).
/// Both SKUs' readers return this shape, so the payload built from it is one builder's output.</summary>
internal sealed record FileIoSeries(
    string DatabaseName, string FileType, string? FileName, long Files, long StallMs, long Ops, long SeriesRank);

/// <summary>One bucketed file I/O point for one series (#3897). Latencies are the bucket's summed stall over
/// its summed operations — null over no operations — and the peaks are the worst single collection's.</summary>
internal sealed record FileIoPoint(
    DateTime BucketStart, string DatabaseName, string FileType, string? FileName,
    long Reads, long Writes, double StallReadMs, double StallWriteMs,
    double? AvgReadLatencyMs, double? AvgWriteLatencyMs, double? PeakReadLatencyMs, double? PeakWriteLatencyMs);

/// <summary>One LCK% wait type over the whole window (#3897): its summed wait over the collections whose
/// interval was knowable, the seconds those covered, and its worst single collection's rate.</summary>
internal sealed record LockWaitType(string WaitType, double TotalWaitMs, double RatedSeconds, double? PeakWaitTimeMsPerSecond);

/// <summary>One bucketed point of the lock-wait FAMILY (#3897): every LCK% type's wait summed per collection,
/// rated over the collection's interval, then time-weighted across the bucket; the peak is the worst single
/// collection's family rate.</summary>
internal sealed record LockWaitPoint(DateTime BucketStart, double WaitTimeMsPerSecond, double? PeakWaitTimeMsPerSecond);

/// <summary>One bucket of one wait type (#3960): the bucket's summed wait and signal wait over the seconds its
/// rated collections covered, and the worst single collection's wait rate.</summary>
internal sealed record WaitBucketPoint(DateTime BucketStart, double WaitTimeMsPerSecond, double SignalWaitTimeMsPerSecond, double? PeakWaitTimeMsPerSecond);

/// <summary>One bucket of CPU samples (#3960): each percentage averaged over the samples in the bucket, the busiest
/// single sample's SQL and total CPU, and how many samples the bucket held.</summary>
internal sealed record CpuBucketPoint(
    DateTime BucketStart, double SqlServerCpu, double OtherProcessCpu, double TotalCpu, double IdleCpu,
    int PeakSqlServerCpu, int PeakTotalCpu, long Samples);

/// <summary>One bucket of tempdb samples (#3960): each space figure averaged, the fullest collection's reserved and
/// version-store space, and the most sessions and the single largest consumer any collection in it saw.</summary>
internal sealed record TempDbBucketPoint(
    DateTime BucketStart, double UserObjectsMb, double InternalObjectsMb, double VersionStoreMb, double TotalReservedMb,
    double UnallocatedMb, double PeakTotalReservedMb, double PeakVersionStoreMb, long SessionsUsingTempDb,
    int TopConsumerSessionId, double TopConsumerMb);

/// <summary>One bucket of memory samples (#3960): each level averaged over the samples in the bucket.</summary>
internal sealed record MemoryBucketPoint(DateTime BucketStart, double TotalServerMemoryMb, double TargetServerMemoryMb, double BufferPoolMb, double PlanCacheMb);

/// <summary>One bucket of the memory-grant series (#3960): the average and the largest total granted across the grant
/// snapshots that fell inside it.</summary>
internal sealed record GrantBucketPoint(DateTime BucketStart, double AvgGrantedMb, double PeakGrantedMb);

/// <summary>
/// One bucket of a perfmon counter (#3960), its instances summed per collection first. The bucket carries every
/// reading the envelope may need, because which one a point publishes depends on the counter's kind
/// (<see cref="DeltaSeriesShaping.BasisFor(string?, int?)"/>): the average and the largest reading for a level, the
/// last reading for a cumulative counter, and the deltas over the seconds they accrued — the rated collections'
/// (interval &gt; 0) where any exist, the unknowable collections' (interval 0) where only those do, and the unrecorded
/// ones' (interval NULL, a pre-V127 row) otherwise, each the class's own sum so a caller's delta / interval stays
/// the formula it always was.
/// </summary>
internal sealed record PerfmonBucketPoint(
    DateTime BucketStart, double AvgValue, long MaxValue, long LastValue,
    long? RatedDelta, long? RatedSeconds, long UnknowableCollections, long? UnknowableDelta, long? UnrecordedDelta,
    double? PeakPerSecond, int? CntrType);

/// <summary>
/// The wire shapes of the bucketed trends both SKUs serve with the same fields — <c>get_file_io_trend</c> and
/// <c>get_lock_wait_trend</c> (#3897), and the wait, CPU, tempdb, memory and perfmon trends (#3960). Built here,
/// once, from the records above, so Lite and Darling cannot publish different keys, orders or sentences for the
/// same read: the parity their tool bodies used to keep by copying is now kept by construction.
/// </summary>
internal static class TrendPayloads
{
    /// <summary>How many ranked file I/O series keep their own line; the rest fold into one "(other)" line.</summary>
    public const int ChartedSeries = 5;

    /// <summary>The label every dimension of the folded line takes — the custom-view composer's residual label
    /// (<c>ComposeCompiler.OtherSeriesLabel</c>), so the web viewer names a residual one way.</summary>
    public const string OtherLabel = "(other)";

    /// <summary>
    /// How many ranked series keep their own line: all of them when folding would leave a single series in the
    /// "(other)" line — a line pooling one series is that series under a worse name — else <see cref="ChartedSeries"/>.
    /// </summary>
    public static int ChartedFor(int activeSeries) =>
        activeSeries <= ChartedSeries + 1 ? activeSeries : ChartedSeries;

    /// <summary>How many lines the answer draws: the charted series, plus the "(other)" line when anything folds.</summary>
    public static int LinesFor(int activeSeries) =>
        ChartedFor(activeSeries) + (activeSeries > ChartedFor(activeSeries) ? 1 : 0);

    /// <summary>
    /// <c>get_file_io_trend</c>'s data envelope. <paramref name="scope"/> is the database the read was narrowed
    /// to (per-file grain), null for the per-(database, file type) grain. The <c>series</c> legend's totals are
    /// summed from the served points, so the legend and the lines cannot disagree about a series; its file counts
    /// come from the ranking.
    /// </summary>
    public static string FileIoTrend(
        string serverName, int hoursBack, string? scope, IReadOnlyList<FileIoSeries> series, IReadOnlyList<FileIoPoint> points,
        int bucketMinutes, bool requested, int autoBudget, object discontinuities)
    {
        var charted = ChartedFor(series.Count);
        var folded = series.Count - charted;
        var perFile = scope is not null;

        var legend = new List<object>();
        foreach (var s in series.Take(charted))
        {
            legend.Add(Legend(s.DatabaseName, s.FileType, s.FileName, s.Files, perFile,
                points.Where(p => p.DatabaseName == s.DatabaseName && p.FileType == s.FileType && p.FileName == s.FileName)));
        }

        if (folded > 0)
        {
            legend.Add(Legend(OtherLabel, OtherLabel, perFile ? OtherLabel : null, series.Skip(charted).Sum(s => s.Files), perFile,
                points.Where(p => p.DatabaseName == OtherLabel && p.FileType == OtherLabel)));
        }

        var envelope = new Dictionary<string, object?>
        {
            ["server"] = serverName,
            ["hours_back"] = hoursBack,
            ["database_name"] = scope,
            ["series_grain"] = perFile ? "file" : "database_file_type",
            ["bucket"] = TrendBuckets.Word(bucketMinutes),
            ["bucket_minutes"] = bucketMinutes,
            ["aggregate_note"] = TrendBuckets.AggregateNote(bucketMinutes, requested, autoBudget),
            ["series_active"] = series.Count,
            ["series_charted"] = charted,
            ["series_folded"] = folded,
            ["series_note"] = FileIoSeriesNote(charted, folded, perFile),
            ["series"] = legend,
            ["trend"] = points.Select(p => perFile
                ? (object)new
                {
                    time = Stamp(p.BucketStart),
                    database_name = p.DatabaseName,
                    file_type = p.FileType,
                    file_name = p.FileName,
                    reads = p.Reads,
                    writes = p.Writes,
                    avg_read_latency_ms = Round2(p.AvgReadLatencyMs),
                    avg_write_latency_ms = Round2(p.AvgWriteLatencyMs),
                    peak_read_latency_ms = Round2(p.PeakReadLatencyMs),
                    peak_write_latency_ms = Round2(p.PeakWriteLatencyMs),
                }
                : new
                {
                    time = Stamp(p.BucketStart),
                    database_name = p.DatabaseName,
                    file_type = p.FileType,
                    reads = p.Reads,
                    writes = p.Writes,
                    avg_read_latency_ms = Round2(p.AvgReadLatencyMs),
                    avg_write_latency_ms = Round2(p.AvgWriteLatencyMs),
                    peak_read_latency_ms = Round2(p.PeakReadLatencyMs),
                    peak_write_latency_ms = Round2(p.PeakWriteLatencyMs),
                }),
            /* #3653 A5: trailing, after the points, on the data envelope only. */
            ["discontinuities"] = discontinuities,
        };

        return JsonSerializer.Serialize(envelope, McpHelpers.JsonOptions);
    }

    /// <summary>What a line is and how the lines were chosen — the one place the ranking and the fold are said.</summary>
    private static string FileIoSeriesNote(int charted, int folded, bool perFile)
    {
        var grain = perFile
            ? "Each line is one file of the requested database"
            : "Each line is one database's data or log files, pooled (file_type ROWS or LOG); pass database_name to chart one database per file";
        var fold = folded > 0
            ? $" The {charted.ToString(CultureInfo.InvariantCulture)} with the most I/O stall in the window (read + write ms) have their own line; the other {folded.ToString(CultureInfo.InvariantCulture)} are folded into one '{OtherLabel}' line, pooled collection by collection, so its latency is theirs combined."
            : " Every line that did I/O in the window is shown, heaviest stall first.";
        return grain + "." + fold;
    }

    /// <summary>One legend row: the series' window totals, summed from its own served points.</summary>
    private static object Legend(string databaseName, string fileType, string? fileName, long files, bool perFile, IEnumerable<FileIoPoint> ownPoints)
    {
        var mine = ownPoints.ToList();
        var reads = mine.Sum(p => p.Reads);
        var writes = mine.Sum(p => p.Writes);
        var stallRead = mine.Sum(p => p.StallReadMs);
        var stallWrite = mine.Sum(p => p.StallWriteMs);
        var avgRead = reads > 0 ? stallRead / reads : (double?)null;
        var avgWrite = writes > 0 ? stallWrite / writes : (double?)null;
        var peakRead = mine.Max(p => p.PeakReadLatencyMs);
        var peakWrite = mine.Max(p => p.PeakWriteLatencyMs);

        return perFile
            ? new
            {
                database_name = databaseName,
                file_type = fileType,
                file_name = fileName,
                files,
                reads,
                writes,
                avg_read_latency_ms = Round2(avgRead),
                avg_write_latency_ms = Round2(avgWrite),
                peak_read_latency_ms = Round2(peakRead),
                peak_write_latency_ms = Round2(peakWrite),
            }
            : new
            {
                database_name = databaseName,
                file_type = fileType,
                files,
                reads,
                writes,
                avg_read_latency_ms = Round2(avgRead),
                avg_write_latency_ms = Round2(avgWrite),
                peak_read_latency_ms = Round2(peakRead),
                peak_write_latency_ms = Round2(peakWrite),
            };
    }

    /// <summary>
    /// The empty answer for a read scoped to one database that saw no I/O in the window. The unscoped empty
    /// answers keep their sentences (the collector-level two states); this one is about the NAME, because a
    /// misspelt database and an idle one both land here and only a list of the real names tells them apart.
    /// </summary>
    public static string FileIoScopeEmptyMessage(string serverName, string scope, int hoursBack) =>
        $"No read or write activity was recorded for database '{scope}' on {serverName} in the last {hoursBack} hour(s). The name must match a collected database exactly (get_file_io_stats lists them); omit database_name for every database.";

    /// <summary>
    /// <c>get_lock_wait_trend</c>'s data envelope: the family's bucketed rate, and the per-type legend that says
    /// which LCK types made it up. <paramref name="types"/> is every LCK% type collected in the window; the ones
    /// that never waited are counted, not listed.
    /// </summary>
    public static string LockWaitTrend(
        string serverName, int hoursBack, IReadOnlyList<LockWaitType> types, IReadOnlyList<LockWaitPoint> points,
        int bucketMinutes, bool requested, int autoBudget)
    {
        var waited = types.Where(t => t.TotalWaitMs > 0)
            .OrderByDescending(t => t.TotalWaitMs)
            .ThenBy(t => t.WaitType, StringComparer.Ordinal)
            .ToList();
        var idle = types.Count - waited.Count;

        var envelope = new Dictionary<string, object?>
        {
            ["server"] = serverName,
            ["hours_back"] = hoursBack,
            ["bucket"] = TrendBuckets.Word(bucketMinutes),
            ["bucket_minutes"] = bucketMinutes,
            ["aggregate_note"] = TrendBuckets.AggregateNote(bucketMinutes, requested, autoBudget),
            ["wait_types_idle"] = idle,
            ["wait_types_note"] = waited.Count > 0
                ? $"trend is the whole LCK% family summed; wait_types lists every type that waited in the window, heaviest first{(idle > 0 ? $", and {idle.ToString(CultureInfo.InvariantCulture)} more were collected and never waited" : string.Empty)}. Hand one to get_wait_trend to chart it alone."
                : $"No LCK% type waited in this window: the {idle.ToString(CultureInfo.InvariantCulture)} collected all stayed at zero, so the trend below is a measured zero, not missing data.",
            ["wait_types"] = waited.Select(t => new
            {
                wait_type = t.WaitType,
                total_wait_ms = Math.Round(t.TotalWaitMs, 0),
                /* Four places, not the trend's three: a type that waited 24 ms across a day is 0.0003 ms/sec, and
                   rounded to three it would read as a type that never waited — which is what wait_types_idle
                   counts. */
                wait_time_ms_per_second = t.RatedSeconds > 0 ? Math.Round(t.TotalWaitMs / t.RatedSeconds, 4) : (double?)null,
                peak_wait_time_ms_per_second = t.PeakWaitTimeMsPerSecond is { } peak ? Math.Round(peak, 4) : (double?)null,
            }),
            ["trend"] = points.Select(p => new
            {
                collection_time = Stamp(p.BucketStart),
                wait_time_ms_per_second = Math.Round(p.WaitTimeMsPerSecond, 3),
                peak_wait_time_ms_per_second = p.PeakWaitTimeMsPerSecond is { } peak ? Math.Round(peak, 3) : (double?)null,
            }),
        };

        return JsonSerializer.Serialize(envelope, McpHelpers.JsonOptions);
    }

    /// <summary>
    /// <c>get_wait_trend</c>'s data envelope (#3960), both SKUs: one wait type's bucketed rate, its signal share, and
    /// the worst single collection as the peak. The keys the per-collection payload carried keep their names; a
    /// point is now a bucket, and the disclosure block says so.
    /// </summary>
    public static string WaitTrend(
        string serverName, string waitType, int hoursBack, IReadOnlyList<WaitBucketPoint> points,
        int bucketMinutes, bool requested, int autoBudget, object discontinuities)
    {
        var envelope = new Dictionary<string, object?>
        {
            ["server"] = serverName,
            ["wait_type"] = waitType,
            ["hours_back"] = hoursBack,
            ["bucket"] = TrendBuckets.Word(bucketMinutes),
            ["bucket_minutes"] = bucketMinutes,
            ["aggregate_note"] = TrendBuckets.AggregateNote(bucketMinutes, requested, autoBudget),
            ["trend"] = points.Select(p => new
            {
                time = Stamp(p.BucketStart),
                wait_time_ms_per_second = p.WaitTimeMsPerSecond,
                signal_wait_time_ms_per_second = p.SignalWaitTimeMsPerSecond,
                peak_wait_time_ms_per_second = p.PeakWaitTimeMsPerSecond,
            }),
            /* #3653 A5: trailing, after the points, on the data envelope only. */
            ["discontinuities"] = discontinuities,
        };

        return JsonSerializer.Serialize(envelope, McpHelpers.JsonOptions);
    }

    /// <summary>The source-cadence sentence <c>get_cpu_utilization</c> has carried since #3653 A15/A16, now beside
    /// the bucket disclosure: the one place both SKUs spell it.</summary>
    public const string CpuCadenceNote =
        "Source cadence: one RING_BUFFER_SCHEDULER_MONITOR record per minute on-prem, Managed Instance and RDS; one sys.dm_db_resource_stats row per 15 seconds on Azure SQL DB. samples_in_bucket is the measured count per bucket.";

    /// <summary>
    /// <c>get_cpu_utilization</c>'s data envelope (#3960), both SKUs. The percentages are the bucket's averages,
    /// rounded to whole percents as they always were; the peaks keep the busiest sample, so a one-minute spike
    /// survives a ten-minute bucket. A point sits on its samples' own clock, not clamped to the window's start.
    /// </summary>
    public static string CpuUtilization(
        string serverName, int hoursBack, IReadOnlyList<CpuBucketPoint> points, int bucketMinutes, bool requested, int autoBudget)
    {
        var envelope = new Dictionary<string, object?>
        {
            ["server"] = serverName,
            ["hours_back"] = hoursBack,
            ["bucket"] = TrendBuckets.Word(bucketMinutes),
            ["bucket_minutes"] = bucketMinutes,
            ["aggregate_note"] = TrendBuckets.LevelNote(bucketMinutes, requested, autoBudget, firstAtWindowStart: false),
            ["note"] = CpuCadenceNote,
            ["samples"] = points.Select(p => new
            {
                sample_time = Stamp(p.BucketStart),
                sql_server_cpu = (int)Math.Round(p.SqlServerCpu),
                other_process_cpu = (int)Math.Round(p.OtherProcessCpu),
                total_cpu = (int)Math.Round(p.TotalCpu),
                idle_cpu = (int)Math.Round(p.IdleCpu),
                peak_sql_server_cpu = p.PeakSqlServerCpu,
                peak_total_cpu = p.PeakTotalCpu,
                samples_in_bucket = p.Samples,
            }),
        };

        return JsonSerializer.Serialize(envelope, McpHelpers.JsonOptions);
    }

    /// <summary>What a tempdb point's session and consumer fields are (#3960): the one sentence the level note cannot
    /// say for them, because they are the bucket's busiest collection, not averages.</summary>
    public const string TempDbFiguresNote =
        "sessions_using_tempdb is the most sessions any collection in the bucket saw, and top_consumer_* the single largest consumer.";

    /// <summary><c>get_tempdb_trend</c>'s data envelope (#3960), both SKUs.</summary>
    public static string TempDbTrend(
        string serverName, int hoursBack, IReadOnlyList<TempDbBucketPoint> points, int bucketMinutes, bool requested, int autoBudget)
    {
        var envelope = new Dictionary<string, object?>
        {
            ["server"] = serverName,
            ["hours_back"] = hoursBack,
            ["bucket"] = TrendBuckets.Word(bucketMinutes),
            ["bucket_minutes"] = bucketMinutes,
            ["aggregate_note"] = TrendBuckets.LevelNote(bucketMinutes, requested, autoBudget),
            ["note"] = TempDbFiguresNote,
            ["trend"] = points.Select(p => new
            {
                time = Stamp(p.BucketStart),
                user_objects_mb = Math.Round(p.UserObjectsMb, 1),
                internal_objects_mb = Math.Round(p.InternalObjectsMb, 1),
                version_store_mb = Math.Round(p.VersionStoreMb, 1),
                total_reserved_mb = Math.Round(p.TotalReservedMb, 1),
                unallocated_mb = Math.Round(p.UnallocatedMb, 1),
                peak_total_reserved_mb = Math.Round(p.PeakTotalReservedMb, 1),
                peak_version_store_mb = Math.Round(p.PeakVersionStoreMb, 1),
                sessions_using_tempdb = p.SessionsUsingTempDb,
                top_consumer_session_id = p.TopConsumerSessionId,
                top_consumer_mb = Math.Round(p.TopConsumerMb, 1),
            }),
        };

        return JsonSerializer.Serialize(envelope, McpHelpers.JsonOptions);
    }

    /// <summary>Why a memory point's grant figures are null, stated once per payload and only when one is (#3548,
    /// #3960): the grant series is joined by bucket now, not by the nearest snapshot.</summary>
    public const string GrantGapNote =
        "total_granted_mb is the average of the memory-grant snapshots inside the bucket and peak_granted_mb the largest; both are null where no snapshot fell inside the bucket - memory_grant_stats is collected on its own schedule, so a gap means no grant measurement then, not zero granted. Use get_memory_grants for the full grant picture.";

    /// <summary>
    /// <c>get_memory_trend</c>'s data envelope (#3960), both SKUs: the memory levels averaged per bucket, and the
    /// memory-grant series gathered into the SAME buckets and joined on them (#3548's join, by bucket rather than by
    /// the nearest snapshot within 30 seconds): average and peak granted, null — never 0 — where no grant snapshot fell
    /// inside the bucket.
    /// </summary>
    public static string MemoryTrend(
        string serverName, int hoursBack, IReadOnlyList<MemoryBucketPoint> points, IReadOnlyList<GrantBucketPoint> grants,
        int bucketMinutes, bool requested, int autoBudget, object discontinuities)
    {
        var granted = grants.ToDictionary(g => g.BucketStart);
        var trend = points.Select(p =>
        {
            granted.TryGetValue(p.BucketStart, out var g);
            return new
            {
                time = Stamp(p.BucketStart),
                total_server_memory_mb = Math.Round(p.TotalServerMemoryMb, 2),
                target_server_memory_mb = Math.Round(p.TargetServerMemoryMb, 2),
                buffer_pool_mb = Math.Round(p.BufferPoolMb, 2),
                plan_cache_mb = Math.Round(p.PlanCacheMb, 2),
                total_granted_mb = g is null ? (double?)null : Math.Round(g.AvgGrantedMb, 2),
                peak_granted_mb = g is null ? (double?)null : Math.Round(g.PeakGrantedMb, 2),
            };
        }).ToList();

        var envelope = new Dictionary<string, object?>
        {
            ["server"] = serverName,
            ["hours_back"] = hoursBack,
            ["bucket"] = TrendBuckets.Word(bucketMinutes),
            ["bucket_minutes"] = bucketMinutes,
            ["aggregate_note"] = TrendBuckets.LevelNote(bucketMinutes, requested, autoBudget),
        };

        /* The note exists to explain null points; a fully covered window gets no note at all rather than a
           null-valued key (JsonOptions writes nulls). */
        if (trend.Any(t => t.total_granted_mb is null))
        {
            envelope["granted_note"] = GrantGapNote;
        }

        envelope["trend"] = trend;
        envelope["discontinuities"] = discontinuities;
        return JsonSerializer.Serialize(envelope, McpHelpers.JsonOptions);
    }

    /// <summary>
    /// <c>get_perfmon_trend</c>'s data envelope (#3960), both SKUs. The series' kind decides what a point's number
    /// is (<see cref="DeltaSeriesShaping.BasisFor(string?, int?)"/>): a LEVEL (a gauge) publishes the bucket's
    /// average reading as <c>value</c> and its largest as <c>peak_value</c>, with the null delta and interval a
    /// gauge always carried; anything cumulative publishes the bucket's LAST reading as <c>value</c> and its deltas
    /// summed over the seconds they accrued as <c>delta_value</c> / <c>sample_interval_seconds</c>, so the
    /// per-second figure is still delta over interval and a bucket whose every collection was unknowable still says
    /// so with an interval of 0; a RATE adds <c>peak_per_second</c>, its busiest single collection.
    /// </summary>
    public static string PerfmonTrend(
        string serverName, string counterName, int hoursBack, IReadOnlyList<PerfmonBucketPoint> points,
        int bucketMinutes, bool requested, int autoBudget, object discontinuities)
    {
        var seriesType = points.Select(p => p.CntrType).LastOrDefault(t => t.HasValue);
        var basis = DeltaSeriesShaping.BasisFor(counterName, seriesType);

        var envelope = new Dictionary<string, object?>
        {
            ["server"] = serverName,
            ["counter_name"] = counterName,
            ["cntr_type"] = seriesType,
            ["counter_kind"] = PerfmonCounterTypes.Word(seriesType),
            ["hours_back"] = hoursBack,
            ["bucket"] = TrendBuckets.Word(bucketMinutes),
            ["bucket_minutes"] = bucketMinutes,
            ["aggregate_note"] = basis == DeltaBasis.Level
                ? TrendBuckets.LevelNote(bucketMinutes, requested, autoBudget)
                : TrendBuckets.AggregateNote(bucketMinutes, requested, autoBudget),
            ["trend"] = points.Select(p => PerfmonPoint(p, basis)),
            ["discontinuities"] = discontinuities,
        };

        return JsonSerializer.Serialize(envelope, McpHelpers.JsonOptions);
    }

    /// <summary>One perfmon point in its counter kind's shape: the peak key only where the kind has one, so a caller
    /// never reads a null peak as a measured absence.</summary>
    private static Dictionary<string, object?> PerfmonPoint(PerfmonBucketPoint p, DeltaBasis basis)
    {
        if (basis == DeltaBasis.Level)
        {
            return new Dictionary<string, object?>
            {
                ["time"] = Stamp(p.BucketStart),
                ["value"] = Math.Round(p.AvgValue, 2),
                ["delta_value"] = null,
                ["sample_interval_seconds"] = null,
                ["peak_value"] = p.MaxValue,
            };
        }

        var (delta, seconds) = PerfmonDeltas(p);
        var point = new Dictionary<string, object?>
        {
            ["time"] = Stamp(p.BucketStart),
            ["value"] = p.LastValue,
            ["delta_value"] = delta,
            ["sample_interval_seconds"] = seconds,
        };

        if (basis == DeltaBasis.PerSecond)
        {
            point["peak_per_second"] = p.PeakPerSecond is { } peak ? Math.Round(peak, 4) : null;
        }

        return point;
    }

    /// <summary>A cumulative counter's delta and the seconds it accrued over, one class at a time: the rated
    /// collections' where any exist; else the unknowable ones' with an interval of 0, the marker a caller must not
    /// divide by; else the unrecorded (pre-V127) ones' with no interval at all. Each delta is the class's own sum,
    /// null where the collector stored none.</summary>
    private static (long? Delta, long? Seconds) PerfmonDeltas(PerfmonBucketPoint p) =>
        p.RatedSeconds is > 0 ? (p.RatedDelta, p.RatedSeconds)
        : p.UnknowableCollections > 0 ? (p.UnknowableDelta, 0)
        : (p.UnrecordedDelta, null);

    /// <summary>A point's stamp in the store's own frame (naive UTC, printed like every collection time).</summary>
    private static string Stamp(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Unspecified).ToString("o", CultureInfo.InvariantCulture);

    private static double? Round2(double? value) => value.HasValue ? Math.Round(value.Value, 2) : null;
}
