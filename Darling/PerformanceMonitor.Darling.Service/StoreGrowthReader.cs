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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>One size series of the store: the whole store or one object. <see cref="Sizes"/>[k] is the size k days before the newest
/// hourly sample (index 0 is the newest), or null when no sample sits within 3 hours of that point.</summary>
internal sealed record StoreGrowthSeries(string Name, bool IsStore, long?[] Sizes);

/// <summary>What one read of <c>collect.store_metrics</c> found: the whole store and each object, sampled once per day.</summary>
internal sealed record StoreGrowthReading(IReadOnlyList<StoreGrowthSeries> Series);

/// <summary>One line the Store Growth alert judged: the last day's growth and the object's own typical day, both in bytes.
/// <see cref="BaselineBytes"/> is null when fewer than <see cref="StoreGrowthJudge.MinBaselineDays"/> daily values exist.</summary>
internal sealed record StoreGrowthLine(string Name, bool IsStore, long GrowthBytes, long? BaselineBytes, bool Crossed);

/// <summary>The verdict of one judgment: the whole store's line, and the objects that crossed their own line, largest growth first.</summary>
internal sealed record StoreGrowthVerdict(StoreGrowthLine? Store, IReadOnlyList<StoreGrowthLine> Objects)
{
    public bool Crossed => Store is { Crossed: true } || Objects.Count > 0;
}

/// <summary>The Store Growth settings read at each judgment: the switch and the two daily growth floors in GB (0 removes a floor).</summary>
internal sealed record StoreGrowthSettings(bool Enabled, int StoreFloorGb, int ObjectFloorGb);

/// <summary>The pure judgment behind the Store Growth alert (#9000).</summary>
internal static class StoreGrowthJudge
{
    /// <summary>A line fires when the last day's growth is at least this many times its own baseline.</summary>
    internal const double BaselineMultiple = 3.0;

    /// <summary>The number of daily growths before the last 24 hours that make the baseline.</summary>
    internal const int BaselineDays = 14;

    /// <summary>Fewer daily values than this and there is no baseline, so no alert.</summary>
    internal const int MinBaselineDays = 7;

    private const double BytesPerGb = 1024.0 * 1024.0 * 1024.0;

    /// <summary>Growth over one day: the size at <paramref name="day"/> days back minus the size one day further back. Negative growth
    /// (pruning, vacuum) counts as zero. Null when either sample is missing.</summary>
    internal static long? DayGrowth(long?[] sizes, int day) =>
        day + 1 < sizes.Length && sizes[day] is long now && sizes[day + 1] is long before ? Math.Max(0, now - before) : null;

    /// <summary>The median of the daily growths before the last 24 hours, or null with fewer than <see cref="MinBaselineDays"/> values.</summary>
    internal static long? Baseline(long?[] sizes)
    {
        var days = new List<long>();
        for (var day = 1; day <= BaselineDays; day++)
        {
            if (DayGrowth(sizes, day) is long growth)
            {
                days.Add(growth);
            }
        }

        if (days.Count < MinBaselineDays)
        {
            return null;
        }

        days.Sort();
        var mid = days.Count / 2;
        return days.Count % 2 == 1 ? days[mid] : (days[mid - 1] + days[mid]) / 2;
    }

    internal static StoreGrowthLine? Judge(StoreGrowthSeries series, int floorGb)
    {
        if (DayGrowth(series.Sizes, 0) is not long growth)
        {
            return null;
        }

        var baseline = Baseline(series.Sizes);
        var crossed = baseline is long b
            && growth > 0
            && growth >= BaselineMultiple * b
            && (floorGb <= 0 || growth >= floorGb * BytesPerGb);
        return new StoreGrowthLine(series.Name, series.IsStore, growth, baseline, crossed);
    }

    internal static StoreGrowthVerdict Judge(StoreGrowthReading reading, StoreGrowthSettings settings)
    {
        StoreGrowthLine? store = null;
        var objects = new List<StoreGrowthLine>();
        foreach (var series in reading.Series)
        {
            if (series.IsStore)
            {
                store = Judge(series, settings.StoreFloorGb);
            }
            else if (Judge(series, settings.ObjectFloorGb) is { Crossed: true } line)
            {
                objects.Add(line);
            }
        }

        objects.Sort((a, b) => b.GrowthBytes.CompareTo(a.GrowthBytes));
        return new StoreGrowthVerdict(store, objects);
    }
}

/// <summary>The one read behind the Store Growth self-alert (#9000): the hourly sizes <see cref="StoreSelfMetrics"/> records, picked once per
/// day (16 points, the newest sample and every 24 hours back from it) for the whole store and for each object that carries a size.</summary>
internal static class StoreGrowthReader
{
    private const int Points = StoreGrowthJudge.BaselineDays + 2;

    /// <summary>Anchors are the newest whole-store sample and every 24 hours before it. Each anchor reads the samples within 3 hours of it
    /// through <c>idx_store_metrics_time</c>, and the nearest sample per object wins. The lump kinds ('other', 'system') are not objects.</summary>
    internal static readonly string Sql = $@"
WITH latest AS (
    SELECT metric_time AS t0
    FROM collect.store_metrics
    WHERE object_kind = '{StoreSelfMetrics.StoreObjectKind}' AND total_bytes IS NOT NULL
    ORDER BY metric_time DESC
    LIMIT 1
), anchors AS (
    SELECT k, t0 - k * interval '24 hours' AS t
    FROM latest, generate_series(0, {Points - 1}) AS k
)
SELECT a.k, m.object_name, m.object_kind, m.total_bytes,
       abs(extract(epoch FROM (m.metric_time - a.t)))::double precision AS distance_seconds
FROM anchors a
JOIN collect.store_metrics m
  ON m.metric_time >= a.t - interval '3 hours' AND m.metric_time < a.t + interval '3 hours'
WHERE m.object_kind IN ('{StoreSelfMetrics.StoreObjectKind}', '{StoreSelfMetrics.HypertableObjectKind}', '{StoreSelfMetrics.DimensionObjectKind}', '{StoreSelfMetrics.ContinuousAggregateObjectKind}', 'table')
  AND m.total_bytes IS NOT NULL";

    /// <summary>Reads the store. Throws on a failed read, so the caller can leave every piece of alert state alone.</summary>
    internal static async Task<StoreGrowthReading> ReadAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(Sql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        var best = new Dictionary<(string Name, bool IsStore), (long?[] Sizes, double[] Distance)>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var k = reader.GetInt32(0);
            var isStore = string.Equals(reader.GetString(2), StoreSelfMetrics.StoreObjectKind, StringComparison.Ordinal);
            var name = isStore ? "store" : reader.GetString(1);
            var distance = reader.GetDouble(4);
            if (!best.TryGetValue((name, isStore), out var entry))
            {
                entry = (new long?[Points], Enumerable.Repeat(double.MaxValue, Points).ToArray());
                best[(name, isStore)] = entry;
            }

            if (distance < entry.Distance[k])
            {
                entry.Distance[k] = distance;
                entry.Sizes[k] = reader.GetInt64(3);
            }
        }

        return new StoreGrowthReading(best.Select(e => new StoreGrowthSeries(e.Key.Name, e.Key.IsStore, e.Value.Sizes)).ToList());
    }

    internal static string FormatGb(long bytes) =>
        (bytes / (1024.0 * 1024.0 * 1024.0)).ToString("0.#", CultureInfo.InvariantCulture) + " GB";
}
