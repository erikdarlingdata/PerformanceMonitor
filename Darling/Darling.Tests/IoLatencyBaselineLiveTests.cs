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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and never touches another test's
   rows, so it is deliberately NOT [Collection("live-postgres")]. The scratch database is dropped on disposal, which is
   the cleanup. */

/// <summary>
/// #8570: the 30-day baseline reads of <see cref="MetricNames.IoLatency"/> and <see cref="MetricNames.IoWriteLatency"/> put
/// every <c>file_io_stats</c> row of the window (about a million on a busy server) through the robust-tier scaffold, and the
/// scaffold's MAD pass expanded those rows three times, sorted them by tier and spilled the sort. Each test runs the shipped
/// statement and the same statement with <see cref="PgBaselineProvider.RobustTierScaffold"/> in place of the wide-window
/// scaffold, over the same rows, on a table of compressed chunks like the store's.
/// </summary>
public sealed class IoLatencyBaselineLiveTests
{
    private const int ServerId = -857_100;

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    private sealed record TierRow(int Hour, int Dow, double? Mean, double? StdDev, long Count, long Days, double? Median, double? Mad);

    private static async Task ExecAsync(NpgsqlConnection c, string sql, CancellationToken ct, params object[] parameters)
    {
        await using var cmd = Bind(c, sql, parameters);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The shipped statement for <paramref name="metric"/>, and the same statement on the scaffold it replaced.</summary>
    private static (string Shipped, string Old) Statements(string metric)
    {
        var shipped = PgBaselineProvider.GetBaselineQuery(metric)!;
        var old = shipped.Replace(PgBaselineProvider.RobustTierScaffoldWide, PgBaselineProvider.RobustTierScaffold, StringComparison.Ordinal);
        Assert.NotEqual(shipped, old);
        return (shipped, old);
    }

    /// <summary>The statement's parameters: the server, the 30-day window, and a clock that steps by an hour inside it, so the local
    /// hour and day of week differ from UTC on both sides of the step.</summary>
    private static object[] Parameters(DateTime end)
        => [ServerId, end.AddDays(-30), end, end.AddDays(-12), -300, -240];

    private static NpgsqlCommand Bind(NpgsqlConnection c, string sql, object[] parameters)
    {
        var cmd = new NpgsqlCommand(sql, c);
        foreach (var p in parameters)
        {
            cmd.Parameters.AddWithValue(p is DateTime dt ? Naive(dt) : p);
        }

        return cmd;
    }

    private static async Task<List<TierRow>> RunAsync(NpgsqlConnection c, string sql, object[] parameters, CancellationToken ct)
    {
        await using var cmd = Bind(c, sql, parameters);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        var rows = new List<TierRow>();
        while (await reader.ReadAsync(ct))
        {
            double? Dbl(int i) => reader.IsDBNull(i) ? null : Convert.ToDouble(reader.GetValue(i), CultureInfo.InvariantCulture);
            rows.Add(new TierRow(reader.GetInt32(0), reader.GetInt32(1), Dbl(2), Dbl(3), reader.GetInt64(4), reader.GetInt64(5), Dbl(6), Dbl(7)));
        }

        return rows.OrderBy(r => r.Hour).ThenBy(r => r.Dow).ToList();
    }

    private static async Task<NpgsqlConnection> PlantAsync(ScratchPostgres scratch, DateTime end, CancellationToken ct)
    {
        var c = new NpgsqlConnection(scratch.ConnectionString);
        await c.OpenAsync(ct);
        await PgMigrations.MigrateAsync(c, ct);
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        await ExecAsync(c, "SELECT _timescaledb_functions.stop_background_workers()", ct);

        /* Migration leaves the tables plain (#7152): the service's conversion step makes file_io_stats a hypertable, and the store
           compresses its old chunks. */
        await TimescaleSupport.ConvertToHypertablesAsync(c, null, ct);
        await ExecAsync(c, TimescaleSupport.EnableCompressionSql(FileIoStatsCollector.Instance), ct);

        /* Twenty-five days of snapshots every five minutes, 60 files each (every fourth a log file, every fifteenth in tempdb),
           about 430,000 rows. Three in ten file rows read nothing and one in five writes nothing, so the read latency is NULL for
           some rows. Every row of one local hour and weekday reads nothing, so one full tier has no value at all. */
        await ExecAsync(c, "SELECT setseed(0.5)", ct);
        await ExecAsync(c, @"
INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, size_mb,
                           delta_reads, delta_writes, delta_stall_read_ms, delta_stall_write_ms)
SELECT 857_100_000_000 + s.k * 100 + f, t.at, $1, 'scale',
       CASE WHEN f % 15 = 0 THEN 'tempdb' ELSE 'Db' || (f / 4) END, 'File' || f, CASE WHEN f % 4 = 3 THEN 'LOG' ELSE 'ROWS' END, 10,
       CASE WHEN random() < 0.3 OR (EXTRACT(HOUR FROM t.local_at) = 17 AND EXTRACT(DOW FROM t.local_at) = 2) THEN 0 ELSE (random() * 5000)::bigint END,
       CASE WHEN random() < 0.2 THEN 0 ELSE 1 + (random() * 3000)::bigint END,
       (random() * random() * 20000)::bigint, (random() * random() * 9000)::bigint
FROM generate_series(0, 7199) AS s (k)
CROSS JOIN LATERAL (SELECT u.at, u.at + CASE WHEN u.at < $2::timestamp - interval '12 days' THEN -300 ELSE -240 END * interval '1 minute' AS local_at
                    FROM (SELECT $2::timestamp - interval '1 minute' - s.k * interval '5 minutes' AS at) AS u) AS t
CROSS JOIN generate_series(0, 59) AS f", ct, ServerId, end);
        await ExecAsync(c, @"
SELECT compress_chunk(ch, if_not_compressed => true)
FROM show_chunks('file_io_stats', older_than => $1::timestamp) AS ch", ct, end.AddDays(-1));
        await ExecAsync(c, "ANALYZE file_io_stats", ct);
        return c;
    }

    private static void AssertSame(IReadOnlyList<TierRow> expected, IReadOnlyList<TierRow> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        for (var i = 0; i < expected.Count; i++)
        {
            var e = expected[i];
            var a = actual[i];
            var tier = $"tier ({e.Hour}, {e.Dow})";
            Assert.True(e.Hour == a.Hour && e.Dow == a.Dow, tier);
            Assert.True(e.Count == a.Count && e.Days == a.Days, tier + " counts");
            /* A median and a MAD are a stored value or the midpoint of two, so they are equal to the last bit. The mean and the sample
               standard deviation are sums of doubles, whose last bits depend on the order the rows reach the aggregate, and that order
               was never fixed. */
            Assert.Equal(e.Median, a.Median);
            Assert.Equal(e.Mad, a.Mad);
            AssertClose(e.Mean, a.Mean, tier + " mean");
            AssertClose(e.StdDev, a.StdDev, tier + " stddev");
        }
    }

    private static void AssertClose(double? expected, double? actual, string what)
    {
        Assert.Equal(expected.HasValue, actual.HasValue);
        if (expected.HasValue)
        {
            Assert.True(Math.Abs(expected.Value - actual!.Value) <= 1e-9 * Math.Max(1.0, Math.Abs(expected.Value)), what + $": {expected} vs {actual}");
        }
    }

    [Theory]
    [InlineData(MetricNames.IoLatency)]
    [InlineData(MetricNames.IoWriteLatency)]
    public async Task TheWideWindowScaffold_ReturnsTheTiersTheOldScaffoldReturned_AgainstDevPostgres(string metric)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live I/O baseline test.");
        var ct = TestContext.Current.CancellationToken;
        var end = LiveClock.AnchoredEnd();
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var c = await PlantAsync(scratch, end, ct);

        var (shipped, old) = Statements(metric);
        var parameters = Parameters(end);
        var expected = await RunAsync(c, old, parameters, ct);
        var actual = await RunAsync(c, shipped, parameters, ct);

        /* 24 hours x 7 days of full tiers, 24 hour tiers and the flat tier. */
        Assert.True(expected.Count >= 190, "the plant should reach every tier, found " + expected.Count);
        AssertSame(expected, actual);

        if (metric == MetricNames.IoLatency)
        {
            /* The full tier of local hour 17 on a Tuesday holds only rows that read nothing: it exists, counted, with no value. */
            var empty = actual.Single(r => r.Hour == 17 && r.Dow == 2);
            Assert.True(empty.Count > 0);
            Assert.Null(empty.Median);
            Assert.Null(empty.Mad);
        }
    }

    /// <summary>Rows that went through a Sort node (actual rows times loops), summed over the plan.</summary>
    private static long SortedRows(JsonElement node)
    {
        var rows = 0L;
        if (node.TryGetProperty("Node Type", out var type) && type.GetString() == "Sort")
        {
            rows += (long)Math.Round(node.GetProperty("Actual Rows").GetDouble() * node.GetProperty("Actual Loops").GetDouble());
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                rows += SortedRows(child);
            }
        }

        return rows;
    }

    private static async Task<(long Sorted, long TempWritten)> MeasureAsync(NpgsqlConnection c, string sql, object[] parameters, CancellationToken ct)
    {
        await using var cmd = Bind(c, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, parameters);
        using var document = JsonDocument.Parse((string)(await cmd.ExecuteScalarAsync(ct))!);
        var plan = document.RootElement[0].GetProperty("Plan");
        return (SortedRows(plan), plan.GetProperty("Temp Written Blocks").GetInt64());
    }

    [Theory]
    [InlineData(MetricNames.IoLatency)]
    [InlineData(MetricNames.IoWriteLatency)]
    public async Task TheWideWindowScaffold_SortsAndSpillsLessThanTheOldScaffold_AgainstDevPostgres(string metric)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live I/O baseline test.");
        var ct = TestContext.Current.CancellationToken;
        var end = LiveClock.AnchoredEnd();
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var c = await PlantAsync(scratch, end, ct);

        /* The server's stock setting, so a plant of this size spills as a busy server's 30 days do. */
        await ExecAsync(c, "SET work_mem = '4MB'", ct);
        var (shipped, old) = Statements(metric);
        var parameters = Parameters(end);
        var before = await MeasureAsync(c, old, parameters, ct);
        var after = await MeasureAsync(c, shipped, parameters, ct);

        Assert.True(before.Sorted > 0 && before.TempWritten > 0, "the old scaffold should sort and spill on this plant: " + before);
        Assert.True(after.Sorted < before.Sorted * 0.85,
            $"the statement sorts {after.Sorted} rows, the old scaffold {before.Sorted}");
        Assert.True(after.TempWritten < before.TempWritten * 0.7,
            $"the statement writes {after.TempWritten} temp blocks, the old scaffold {before.TempWritten}");
    }
}
