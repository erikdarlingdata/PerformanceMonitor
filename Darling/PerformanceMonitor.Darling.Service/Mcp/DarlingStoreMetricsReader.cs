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
/// The store self-metrics reads for the MCP/web surface (#2068) — over the series the hourly
/// <c>StoreSelfMetrics</c> sweep persists into <c>collect.store_metrics</c>. Two reads: the latest
/// snapshot per object (one row per hypertable / dimension / the store itself, at each object's newest
/// metric_time), and the daily series — the LAST sample of each object per day, which is what a
/// growth/forecast question wants (the hourly grain exists so a single missed run costs nothing, not
/// because anyone forecasts by the hour). Plus the one derivable number the issue called out: the
/// per-server daily ingest rate (whole-store daily growth divided by the enabled-server count), computed in
/// <see cref="ComputeDailyGrowth"/> — pure, so it is unit-tested without a store.
/// </summary>
internal static class DarlingStoreMetricsReader
{
    /// <summary>Latest snapshot per object — DISTINCT ON takes each (kind, name)'s newest row. No
    /// parameters: the newest row per object is wanted regardless of window.</summary>
    public const string StoreMetricsLatestSql = @"
SELECT DISTINCT ON (object_kind, object_name)
    object_kind,
    object_name,
    metric_time,
    total_bytes,
    compressed_before_bytes,
    compressed_after_bytes,
    chunk_count,
    row_count,
    enabled_server_count,
    last_run_duration_ms,
    schedule_interval_ms,
    total_runs,
    total_failures
FROM collect.store_metrics
ORDER BY object_kind, object_name, metric_time DESC";

    /// <summary>The daily series — the LAST sample of each object per day (DISTINCT ON over the day
    /// bucket, newest first within it), so each day contributes one settled point per object rather than
    /// 24 near-duplicates. $1 window start (naive UTC).</summary>
    public const string StoreMetricsDailySql = @"
SELECT DISTINCT ON (object_kind, object_name, date_trunc('day', metric_time))
    object_kind,
    object_name,
    date_trunc('day', metric_time) AS day,
    total_bytes,
    compressed_before_bytes,
    compressed_after_bytes,
    chunk_count,
    row_count,
    enabled_server_count,
    last_run_duration_ms,
    schedule_interval_ms,
    total_runs,
    total_failures
FROM collect.store_metrics
WHERE metric_time >= $1
ORDER BY object_kind, object_name, date_trunc('day', metric_time), metric_time DESC";

    /// <summary>One object's newest self-metrics row. The four job fields (#2136, V56) are non-null only
    /// on <c>background_job</c> rows — every other kind leaves them NULL, as the sweep writes them.</summary>
    public sealed record StoreMetricRow(
        string ObjectKind,
        string ObjectName,
        DateTime MetricTime,
        long? TotalBytes,
        long? CompressedBeforeBytes,
        long? CompressedAfterBytes,
        int? ChunkCount,
        long? RowCount,
        int? EnabledServerCount,
        long? LastRunDurationMs = null,
        long? ScheduleIntervalMs = null,
        long? TotalRuns = null,
        long? TotalFailures = null);

    /// <summary>One object's settled point for one day (the day's last sample). Job fields as on
    /// <see cref="StoreMetricRow"/>.</summary>
    public sealed record StoreMetricDailyPoint(
        string ObjectKind,
        string ObjectName,
        DateTime Day,
        long? TotalBytes,
        long? CompressedBeforeBytes,
        long? CompressedAfterBytes,
        int? ChunkCount,
        long? RowCount,
        int? EnabledServerCount,
        long? LastRunDurationMs = null,
        long? ScheduleIntervalMs = null,
        long? TotalRuns = null,
        long? TotalFailures = null);

    /// <summary>One day's whole-store growth: the byte delta from the previous day's settled point, and
    /// that delta divided by the day's enabled-server count — the number onboarding N servers multiplies.
    /// <c>PerServerBytes</c> is null when the server count is unknown or zero (a delta over no servers is
    /// not a rate).</summary>
    public sealed record DailyGrowthPoint(DateTime Day, long DeltaBytes, double? PerServerBytes);

    public static async Task<List<StoreMetricRow>> GetLatestAsync(
        NpgsqlDataSource postgres, CancellationToken cancellationToken = default)
    {
        var rows = new List<StoreMetricRow>();
        await using var command = postgres.CreateCommand(StoreMetricsLatestSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new StoreMetricRow(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetDateTime(2),
                reader.IsDBNull(3) ? null : reader.GetInt64(3),
                reader.IsDBNull(4) ? null : reader.GetInt64(4),
                reader.IsDBNull(5) ? null : reader.GetInt64(5),
                reader.IsDBNull(6) ? null : reader.GetInt32(6),
                reader.IsDBNull(7) ? null : reader.GetInt64(7),
                reader.IsDBNull(8) ? null : reader.GetInt32(8),
                reader.IsDBNull(9) ? null : reader.GetInt64(9),
                reader.IsDBNull(10) ? null : reader.GetInt64(10),
                reader.IsDBNull(11) ? null : reader.GetInt64(11),
                reader.IsDBNull(12) ? null : reader.GetInt64(12)));
        }

        return rows;
    }

    public static async Task<List<StoreMetricDailyPoint>> GetDailyAsync(
        NpgsqlDataSource postgres, DateTime sinceUtc, CancellationToken cancellationToken = default)
    {
        var rows = new List<StoreMetricDailyPoint>();
        await using var command = postgres.CreateCommand(StoreMetricsDailySql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue(DateTime.SpecifyKind(sinceUtc, DateTimeKind.Unspecified));

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new StoreMetricDailyPoint(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetDateTime(2),
                reader.IsDBNull(3) ? null : reader.GetInt64(3),
                reader.IsDBNull(4) ? null : reader.GetInt64(4),
                reader.IsDBNull(5) ? null : reader.GetInt64(5),
                reader.IsDBNull(6) ? null : reader.GetInt32(6),
                reader.IsDBNull(7) ? null : reader.GetInt64(7),
                reader.IsDBNull(8) ? null : reader.GetInt32(8),
                reader.IsDBNull(9) ? null : reader.GetInt64(9),
                reader.IsDBNull(10) ? null : reader.GetInt64(10),
                reader.IsDBNull(11) ? null : reader.GetInt64(11),
                reader.IsDBNull(12) ? null : reader.GetInt64(12)));
        }

        return rows;
    }

    /// <summary>
    /// The whole-store daily growth series from the store-kind daily points, ordered by day: each day's
    /// byte delta from the previous day's settled point, plus the per-server rate (delta divided by THAT
    /// day's enabled-server count — the day being measured, not the baseline day). Pure. The first day has
    /// no predecessor and yields no point; a day whose total or predecessor's total is unrecorded is
    /// skipped rather than invented; the per-server rate is null (never zero, never infinity) when the
    /// server count is missing or zero. Deltas can be NEGATIVE — retention drops and compression passes
    /// shrink the store, and hiding that would misstate the trend a forecast extrapolates.
    /// </summary>
    public static List<DailyGrowthPoint> ComputeDailyGrowth(IReadOnlyList<StoreMetricDailyPoint> storePoints)
    {
        if (storePoints is null)
        {
            throw new ArgumentNullException(nameof(storePoints));
        }

        var growth = new List<DailyGrowthPoint>();
        for (var i = 1; i < storePoints.Count; i++)
        {
            var previous = storePoints[i - 1];
            var current = storePoints[i];
            if (previous.TotalBytes is not { } before || current.TotalBytes is not { } after)
            {
                continue;
            }

            var delta = after - before;
            double? perServer = current.EnabledServerCount is > 0
                ? delta / (double)current.EnabledServerCount.Value
                : null;

            growth.Add(new DailyGrowthPoint(current.Day, delta, perServer));
        }

        return growth;
    }
}
