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
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5521: the hole scan's split over a materialization's compressed boundary. A probe of
/// <c>m.bucket = b.bucket</c> on a compressed chunk is a Seq Scan of the chunk's compressed heap (no index covers a
/// batch's bucket range), run once per bucket, so the hourly scan read about 1.8 M blocks a call on a large store.
/// Below the boundary the scan reads the buckets present once, as a set; at and above it the per-bucket index
/// probe stays. These pins hold the text; the live half proves the same holes against the old statement.
/// </summary>
public sealed class MaterializationHoleScanSplitSqlTests
{
    private static readonly (string Schema, string Name) Materialization = ("_timescaledb_internal", "_materialized_hypertable_42");

    private const string Relation = "\"_timescaledb_internal\".\"_materialized_hypertable_42\"";

    /// <summary>
    /// Every target's split scan: the set read is bounded by the window and the boundary and sits in a
    /// MATERIALIZED CTE (read once), the per-bucket probe is on the one arm filtered to <c>b.bucket &gt;= $4</c>
    /// and is the only place the materialization is probed per bucket, the candidates are fenced, and the raw
    /// probe filters the union.
    /// </summary>
    [Fact]
    public void EveryTargetsSplitScan_ReadsTheSetOnce_AndProbesOnlyAtOrAboveTheBoundary()
    {
        Assert.NotEmpty(TimescaleSupport.MaterializationHoleTargets);

        foreach (var target in TimescaleSupport.MaterializationHoleTargets)
        {
            var sql = TimescaleSupport.MaterializationHoleScanSplitSql(target, Materialization).Replace("\r\n", "\n", StringComparison.Ordinal);

            Assert.Contains($"WITH present AS MATERIALIZED (\n    SELECT DISTINCT m.bucket\n    FROM {Relation} AS m\n    WHERE m.bucket >= $1::timestamp\n    AND   m.bucket <= $2::timestamp\n    AND   m.bucket < $4::timestamp\n)", sql, StringComparison.Ordinal);

            /* The below-boundary arm tests the set and nothing per bucket... */
            var belowAt = sql.IndexOf("WHERE b.bucket < $4::timestamp\n    AND   NOT EXISTS (SELECT 1 FROM present AS p WHERE p.bucket = b.bucket)", StringComparison.Ordinal);
            /* ...and the probe arm filters to the boundary BEFORE the fenced probe. */
            var aboveAt = sql.IndexOf("WHERE b.bucket >= $4::timestamp\n    AND   NOT EXISTS (SELECT 1 FROM " + Relation + " AS m WHERE m.bucket = b.bucket OFFSET 0)", StringComparison.Ordinal);
            var fenceAt = sql.IndexOf("    OFFSET 0\n) AS c\nWHERE EXISTS (", StringComparison.Ordinal);
            var sourceProbeAt = sql.IndexOf($"SELECT 1 FROM collect.{target.Source} AS s", StringComparison.Ordinal);

            Assert.True(belowAt > 0 && aboveAt > belowAt && fenceAt > aboveAt && sourceProbeAt > fenceAt,
                $"{target.View}: set arm, then probe arm, then the candidate fence, then the raw probe");

            /* The materialization is named twice: the CTE (read once) and the probe (one arm). */
            Assert.Equal(2, sql.Split(Relation).Length - 1);

            /* Three fences: the probe, the candidates, the raw probe. */
            Assert.Equal(3, sql.Split("OFFSET 0").Length - 1);
            Assert.EndsWith("    OFFSET 0)\nORDER BY c.bucket", sql.TrimEnd(), StringComparison.Ordinal);
        }
    }

    /// <summary>The boundary read: the newest compressed chunk's end off TimescaleDB's public chunk view, by the
    /// materialization hypertable's own name, as naive UTC.</summary>
    [Fact]
    public void TheBoundaryRead_IsTheNewestCompressedChunkEnd_AsNaiveUtc()
    {
        var sql = TimescaleSupport.CompressedMaterializationBoundarySql;

        Assert.Contains("MAX(ch.range_end) AT TIME ZONE 'UTC'", sql, StringComparison.Ordinal);
        Assert.Contains("FROM timescaledb_information.chunks AS ch", sql, StringComparison.Ordinal);
        Assert.Contains("ch.hypertable_schema = $1", sql, StringComparison.Ordinal);
        Assert.Contains("ch.hypertable_name = $2", sql, StringComparison.Ordinal);
        Assert.Contains("AND   ch.is_compressed", sql, StringComparison.Ordinal);
    }
}

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres, stops the TimescaleDB
   background workers in it so no scheduler call races the chunk-by-chunk compression the states need, and never
   touches another test's rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #5521 against a store: the split scan returns exactly the holes the old statement returns, in every state a
/// materialization can be in, on two targets, and its plan runs the per-bucket probe only for buckets at or above
/// the boundary.
///
/// <para><b>The oracle is the old statement</b> (<see cref="TimescaleSupport.MaterializationHoleScanSql"/>,
/// unchanged). The seed is six UTC days, one chunk a day, collected every hour except two outages (no raw, so not
/// holes). Holes are planted by deleting buckets from the materialization: hour 10 (inside a compressed chunk),
/// 71 and 72 (the last hour below and the first at the boundary the mixed state compresses up to), 73 and 100
/// (uncompressed), and 130 (inside the chunk the last state decompresses). Each state asserts the
/// production scan, and the split statement run directly at several boundaries (the answer does not depend on where
/// the boundary falls), against the oracle, over windows that start inside, end at and straddle the boundary.</para>
/// </summary>
public sealed class MaterializationHoleScanSplitLiveTests
{
    private const int ServerId = -552101;
    private const string ServerName = "hole-scan-split-5521";

    /// <summary>The buckets deleted from every target's materialization after the refresh.</summary>
    private static readonly int[] PlantedHoles = [10, 71, 72, 73, 100, 130];

    /// <summary>Hours with no raw row at all: not holes, but candidates the raw probe must reject.</summary>
    private static readonly int[] Outage = [30, 31, 32, 33, 120, 121];

    private const int Hours = 144;

    [Fact]
    public async Task TheSplitScan_FindsExactlyTheOldScansHoles_InEveryMaterializationState_OnTwoTargets_AgainstDevPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the split hole-scan parity test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.SkipWhen(!await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct),
            "the split hole-scan parity test needs TimescaleDB: a compressed chunk exists only there.");

        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        await Exec(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        /* One day a chunk, as the store runs them, BEFORE any row lands, so the compression below acts on
           whole days. Then compression on each materialization. */
        await TimescaleSupport.EnsureMaterializationChunkIntervalAsync(connection, null, ct);
        await TimescaleSupport.EnsureAggregateCompressionAsync(connection, null, ct);

        /* A fixed past midnight: chunks align to UTC midnight, so hour 24 * k is a chunk edge. */
        var h0 = new DateTime(2026, 3, 2, 0, 0, 0, DateTimeKind.Unspecified);
        DateTime H(int n) => h0.AddHours(n);

        var targets = new[] { TimescaleSupport.QueryStatsIntervalHourlyView, TimescaleSupport.QueryStatsBaselineView }
            .Select(view => TimescaleSupport.MaterializationHoleTargets.Single(t => t.View == view))
            .ToArray();
        var materializations = new Dictionary<string, (string Schema, string Name)>(StringComparer.Ordinal);
        foreach (var target in targets)
        {
            var resolved = await TimescaleSupport.ResolveMaterializationAsync(connection, target.View, ct);
            Assert.NotNull(resolved);
            materializations[target.View] = resolved.Value;
        }

        var collected = Enumerable.Range(0, Hours).Where(h => !Outage.Contains(h)).ToArray();
        await PlantHoursAsync(connection, h0, collected, ct);
        var windowAll = (H(0), H(Hours - 1));

        /* A materialization that is not there reads as no boundary, not an error: the old statement runs. */
        Assert.Null(await TimescaleSupport.ReadCompressedMaterializationBoundaryAsync(connection, ("_timescaledb_internal", "_materialized_hypertable_999999"), ct));

        /* State 1: EMPTY materialization. Every collected hour is a hole; no chunk, so no boundary. */
        foreach (var target in targets)
        {
            Assert.Null(await TimescaleSupport.ReadCompressedMaterializationBoundaryAsync(connection, materializations[target.View], ct));
        }

        await AssertParityAsync(connection, targets, materializations, new[] { windowAll }, collected.Select(H).ToArray(), "empty materialization", ct);

        /* Materialize everything, then punch the planted holes. */
        foreach (var target in targets)
        {
            await Exec(connection, $"CALL refresh_continuous_aggregate('collect.{target.View}'::regclass, '{H(0):yyyy-MM-dd HH:mm:ss}'::timestamp, '{H(Hours):yyyy-MM-dd HH:mm:ss}'::timestamp)", ct);
            await DeleteBucketsAsync(connection, materializations[target.View], PlantedHoles.Select(H).ToArray(), ct);
        }

        var planted = PlantedHoles.Select(H).ToArray();

        /* State 2: NO COMPRESSED CHUNK. The production scan runs the old statement. */
        foreach (var target in targets)
        {
            Assert.Null(await TimescaleSupport.ReadCompressedMaterializationBoundaryAsync(connection, materializations[target.View], ct));
        }

        await AssertParityAsync(connection, targets, materializations, Windows(H), planted, "no compressed chunk", ct);

        /* State 3: MIXED. Days 0-2 compressed: boundary at hour 72, holes inside the compressed part (10, 71), ON the
           boundary hour (72) and above it (73, 100, 130). */
        await CompressThroughAsync(connection, targets, H(72), ct);
        foreach (var target in targets)
        {
            Assert.Equal(H(72), await TimescaleSupport.ReadCompressedMaterializationBoundaryAsync(connection, materializations[target.View], ct));
        }

        await AssertParityAsync(connection, targets, materializations, Windows(H), planted, "mixed: compressed below hour 72", ct);

        /* The pin: the per-bucket probe runs only for the series points at or above the boundary, 72 of 144. */
        foreach (var target in targets)
        {
            Assert.Equal(Hours - 72, await ProbeLoopsAsync(connection, target, materializations[target.View], H(0), H(Hours - 1), H(72), ct));
            Assert.Equal(Hours - 100, await ProbeLoopsAsync(connection, target, materializations[target.View], H(0), H(Hours - 1), H(100), ct));
            Assert.Equal(0, await ProbeLoopsAsync(connection, target, materializations[target.View], H(0), H(71), H(72), ct));
        }

        /* State 4: ALL COMPRESSED. */
        await CompressThroughAsync(connection, targets, H(Hours), ct);
        foreach (var target in targets)
        {
            Assert.Equal(H(Hours), await TimescaleSupport.ReadCompressedMaterializationBoundaryAsync(connection, materializations[target.View], ct));
        }

        await AssertParityAsync(connection, targets, materializations, Windows(H), planted, "all compressed", ct);

        /* State 5: a RECOMPRESSION GAP: day 5 decompressed again (an uncompressed chunk at the top, hole 130 in it),
           day 1 decompressed (an uncompressed chunk BELOW the boundary). The set read still sees its rows. */
        foreach (var target in targets)
        {
            await Exec(connection, $"SELECT decompress_chunk(c) FROM show_chunks('collect.{target.View}', newer_than => '{H(24):yyyy-MM-dd HH:mm:ss}'::timestamp, older_than => '{H(48):yyyy-MM-dd HH:mm:ss}'::timestamp) AS c", ct);
            await Exec(connection, $"SELECT decompress_chunk(c) FROM show_chunks('collect.{target.View}', newer_than => '{H(120):yyyy-MM-dd HH:mm:ss}'::timestamp, older_than => '{H(144):yyyy-MM-dd HH:mm:ss}'::timestamp) AS c", ct);
            Assert.Equal(H(120), await TimescaleSupport.ReadCompressedMaterializationBoundaryAsync(connection, materializations[target.View], ct));
        }

        await AssertParityAsync(connection, targets, materializations, Windows(H), planted, "a decompressed chunk below the boundary and one above", ct);
    }

    /// <summary>
    /// A plain PostgreSQL store (no TimescaleDB extension, so no <c>timescaledb_information.chunks</c>): the
    /// boundary read answers <c>null</c> without an error, and the production scan runs the old statement and
    /// finds its holes. The materialization here is an ordinary table; the store never sees a chunk view.
    /// </summary>
    [Fact]
    public async Task OnAStoreWithoutTimescaleDB_TheBoundaryIsNull_AndTheScanRunsTheOldStatement_AgainstDevPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the no-TimescaleDB split hole-scan fallback test (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        /* Deliberately no CREATE EXTENSION timescaledb: this store has no chunk view to read. */
        Assert.False(await ScalarAsync(connection, "SELECT to_regclass('timescaledb_information.chunks') IS NOT NULL", ct), "the scratch store must have no chunk view");

        await Exec(connection, "CREATE TABLE public.plain_materialization (bucket timestamp NOT NULL)", ct);
        var h0 = new DateTime(2026, 3, 2, 0, 0, 0, DateTimeKind.Unspecified);
        var covered = new[] { 0, 1, 2, 4, 5, 8 };
        await Exec(connection, $"INSERT INTO public.plain_materialization SELECT '{h0:yyyy-MM-dd HH:mm:ss}'::timestamp + make_interval(hours => h) FROM unnest(ARRAY[{string.Join(",", covered)}]) AS h", ct);
        await PlantHoursAsync(connection, h0, [0, 1, 2, 3, 4, 5, 6, 8], ct);

        var target = TimescaleSupport.MaterializationHoleTargets.Single(t => t.View == TimescaleSupport.QueryStatsIntervalHourlyView);
        var materialization = ("public", "plain_materialization");

        Assert.Null(await TimescaleSupport.ReadCompressedMaterializationBoundaryAsync(connection, materialization, ct));

        var holes = await TimescaleSupport.ScanHolesAsync(connection, target, materialization, h0, h0.AddHours(9), ct);
        var oracle = await ScanAsync(connection, TimescaleSupport.MaterializationHoleScanSql(target, materialization), h0, h0.AddHours(9), target.BucketWidth, null, ct);
        Assert.Equal(oracle, holes);
        Assert.Equal(new[] { h0.AddHours(3), h0.AddHours(6) }, holes);
    }

    private static async Task<bool> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>The windows each state is read over: the whole span; starting inside a compressed chunk; ending
    /// exactly one hour below, at and one hour above the 72-hour edge; entirely above it; starting on it; a
    /// single bucket.</summary>
    private static (DateTime From, DateTime To)[] Windows(Func<int, DateTime> h) =>
    [
        (h(0), h(Hours - 1)),
        (h(5), h(Hours - 1)),
        (h(0), h(70)),
        (h(0), h(71)),
        (h(0), h(72)),
        (h(0), h(73)),
        (h(72), h(Hours - 1)),
        (h(73), h(Hours - 1)),
        (h(71), h(72)),
        (h(70), h(75)),
        (h(100), h(100)),
        (h(10), h(10)),
        (h(0), h(Hours + 5)),
    ];

    /// <summary>Asserts, for every target and window, that the production scan and the split statement at several
    /// boundaries return the old statement's holes, and that the old statement returns the holes expected.</summary>
    private static async Task AssertParityAsync(
        NpgsqlConnection connection,
        TimescaleSupport.MaterializationHoleTarget[] targets,
        Dictionary<string, (string Schema, string Name)> materializations,
        (DateTime From, DateTime To)[] windows,
        DateTime[] holesInSpan,
        string state,
        CancellationToken ct)
    {
        foreach (var target in targets)
        {
            var materialization = materializations[target.View];
            foreach (var (from, to) in windows)
            {
                var oracle = await ScanAsync(connection, TimescaleSupport.MaterializationHoleScanSql(target, materialization), from, to, target.BucketWidth, null, ct);
                var expected = holesInSpan.Where(b => b >= from && b <= to).OrderBy(b => b).ToArray();
                Assert.True(expected.SequenceEqual(oracle),
                    $"[{state}] {target.View} {from:HH} .. {to:HH}: the old statement should find {Describe(expected)}, found {Describe(oracle)}");

                var production = await TimescaleSupport.ScanHolesAsync(connection, target, materialization, from, to, ct);
                Assert.True(oracle.SequenceEqual(production),
                    $"[{state}] {target.View} {from:yyyy-MM-dd HH} .. {to:yyyy-MM-dd HH}: production scan {Describe(production)} != old statement {Describe(oracle)}");

                /* The split text is right wherever the boundary falls, compressed chunk or not. */
                foreach (var boundary in new[] { from.AddHours(-1), from, from.AddHours(1), from.AddHours(37), to, to.AddHours(1), to.AddDays(40), from.AddDays(-40) })
                {
                    var split = await ScanAsync(connection, TimescaleSupport.MaterializationHoleScanSplitSql(target, materialization), from, to, target.BucketWidth, boundary, ct);
                    Assert.True(oracle.SequenceEqual(split),
                        $"[{state}] {target.View} {from:yyyy-MM-dd HH} .. {to:yyyy-MM-dd HH} split at {boundary:yyyy-MM-dd HH}: {Describe(split)} != old statement {Describe(oracle)}");
                }
            }
        }
    }

    private static string Describe(IEnumerable<DateTime> buckets)
        => "[" + string.Join(",", buckets.Select(b => b.ToString("MM-dd HH", CultureInfo.InvariantCulture))) + "]";

    /// <summary>
    /// The split statement's plan: how many times the materialization's per-bucket probe ran. The probe hangs off
    /// the <c>generate_series</c> function scan of the arm filtered to the boundary and above; the arm below it
    /// must carry no SubPlan at all.
    /// </summary>
    private static async Task<int> ProbeLoopsAsync(
        NpgsqlConnection connection, TimescaleSupport.MaterializationHoleTarget target, (string Schema, string Name) materialization,
        DateTime from, DateTime to, DateTime boundary, CancellationToken ct)
    {
        await using var explain = new NpgsqlCommand(
            "EXPLAIN (ANALYZE, FORMAT JSON) " + TimescaleSupport.MaterializationHoleScanSplitSql(target, materialization), connection);
        explain.Parameters.AddWithValue(from);
        explain.Parameters.AddWithValue(to);
        explain.Parameters.AddWithValue(target.BucketWidth);
        explain.Parameters.AddWithValue(boundary);
        var json = (string)(await explain.ExecuteScalarAsync(ct))!;
        using var plan = JsonDocument.Parse(json);

        var nodes = Walk(plan.RootElement[0].GetProperty("Plan")).ToArray();
        var seriesScans = nodes.Where(n => NodeType(n) == "Function Scan"
            && n.TryGetProperty("Function Name", out var fn) && fn.GetString() == "generate_series").ToArray();
        Assert.Equal(2, seriesScans.Length);

        var probes = seriesScans.SelectMany(scan => scan.TryGetProperty("Plans", out var children)
            ? children.EnumerateArray().Where(c => c.GetProperty("Parent Relationship").GetString() == "SubPlan")
            : []).ToArray();

        /* Nothing below the boundary is probed: the only SubPlan under a series scan is the probe arm's. A window
           wholly below the boundary leaves that arm without a row to probe, so its SubPlan never runs (loops 0,
           or absent when the planner proved the arm empty). */
        if (probes.Length == 0)
        {
            return 0;
        }

        Assert.Single(probes);
        return probes[0].GetProperty("Actual Loops").GetInt32();
    }

    private static IEnumerable<JsonElement> Walk(JsonElement node)
    {
        yield return node;
        if (node.TryGetProperty("Plans", out var plans))
        {
            foreach (var child in plans.EnumerateArray())
            {
                foreach (var descendant in Walk(child))
                {
                    yield return descendant;
                }
            }
        }
    }

    private static string? NodeType(JsonElement node) => node.GetProperty("Node Type").GetString();

    /// <summary>Compresses every chunk of every target's materialization that ends at or before <paramref name="through"/>.</summary>
    private static async Task CompressThroughAsync(
        NpgsqlConnection connection, TimescaleSupport.MaterializationHoleTarget[] targets, DateTime through, CancellationToken ct)
    {
        foreach (var target in targets)
        {
            await Exec(connection, $"SELECT compress_chunk(c, if_not_compressed => true) FROM show_chunks('collect.{target.View}', older_than => '{through:yyyy-MM-dd HH:mm:ss}'::timestamp) AS c", ct);
        }
    }

    private static async Task DeleteBucketsAsync(NpgsqlConnection connection, (string Schema, string Name) materialization, DateTime[] buckets, CancellationToken ct)
    {
        await using var delete = new NpgsqlCommand($"DELETE FROM \"{materialization.Schema}\".\"{materialization.Name}\" WHERE bucket = ANY($1)", connection);
        delete.Parameters.AddWithValue(buckets);
        await delete.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Three collections an hour, two hashes each, in one statement.</summary>
    private static async Task PlantHoursAsync(NpgsqlConnection connection, DateTime h0, int[] hours, CancellationToken ct)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
SELECT h * 1000 + m * 10 + q, $1 + make_interval(hours => h, mins => m), $2, $3, 'ScanDb', '0xHASH' || q, '0xHANDLE' || q, 1000, 1000, 10, 1200
FROM unnest($4::int[]) AS h, (VALUES (0), (20), (40)) AS c(m), (VALUES (1), (2)) AS k(q)", connection);
        insert.Parameters.AddWithValue(h0);
        insert.Parameters.AddWithValue(ServerId);
        insert.Parameters.AddWithValue(ServerName);
        insert.Parameters.AddWithValue(hours);
        await insert.ExecuteNonQueryAsync(ct);
    }

    private static async Task Exec(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<DateTime[]> ScanAsync(
        NpgsqlConnection connection, string sql, DateTime from, DateTime to, TimeSpan width, DateTime? boundary, CancellationToken ct)
    {
        var buckets = new List<DateTime>();
        await using var scan = new NpgsqlCommand(sql, connection);
        scan.Parameters.AddWithValue(from);
        scan.Parameters.AddWithValue(to);
        scan.Parameters.AddWithValue(width);
        if (boundary is DateTime split)
        {
            scan.Parameters.AddWithValue(split);
        }

        await using var reader = await scan.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            buckets.Add(reader.GetDateTime(0));
        }

        return buckets.ToArray();
    }
}
