/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3933: the start-up materialization-hole scan's read shape. Its two per-bucket probes were written as a bare
/// <c>NOT EXISTS</c> and <c>EXISTS</c>, and PostgreSQL pulled both up into joins that read the whole
/// materialization and the whole source table: the defect #3905 fixed in the daily summary's not-carried probes,
/// here without even a <c>server_id</c> to bound it. On DARLING01 the start-up scan of the wait-stats baseline
/// materialized all 9,785,051 <c>wait_stats</c> rows to disk and read them through once per outage hour: 4.5 s,
/// 64,503 temp blocks, for one aggregate of twenty-three. Fenced, it is 22 ms. These pins hold the fenced shape;
/// the live half proves the same buckets come back against the old text as an oracle, and that the plan runs
/// the probes per bucket.
/// </summary>
public sealed class MaterializationHoleScanShapeSqlTests
{
    /// <summary>
    /// Every target's scan fences both probes and the candidate buckets: three <c>OFFSET 0</c>, the
    /// materialization probe inside the candidate subquery, and the source probe filtering its output, so the
    /// source is probed only for the buckets the materialization probe found empty.
    /// </summary>
    [Fact]
    public void EveryTargetsScan_FencesBothProbes_AndProbesTheSourceOnlyForEmptyBuckets()
    {
        Assert.NotEmpty(TimescaleSupport.MaterializationHoleTargets);

        foreach (var target in TimescaleSupport.MaterializationHoleTargets)
        {
            var sql = TimescaleSupport.MaterializationHoleScanSql(target, ("_timescaledb_internal", "_materialized_hypertable_42"))
                .Replace("\r\n", "\n", StringComparison.Ordinal);

            Assert.Equal(3, sql.Split("OFFSET 0").Length - 1);

            var materializationProbeAt = sql.IndexOf(
                "WHERE NOT EXISTS (SELECT 1 FROM \"_timescaledb_internal\".\"_materialized_hypertable_42\" AS m WHERE m.bucket = b.bucket OFFSET 0)",
                StringComparison.Ordinal);
            var fenceAt = sql.IndexOf("    OFFSET 0\n) AS c\nWHERE EXISTS (", StringComparison.Ordinal);
            var sourceProbeAt = sql.IndexOf($"SELECT 1 FROM collect.{target.Source} AS s", StringComparison.Ordinal);

            Assert.True(materializationProbeAt > 0 && fenceAt > materializationProbeAt && sourceProbeAt > fenceAt,
                $"{target.View}: the materialization probe sits inside the fenced candidate subquery and the source probe filters its output");
            Assert.EndsWith("    OFFSET 0)\nORDER BY c.bucket", sql.TrimEnd(), StringComparison.Ordinal);
        }
    }
}

/// <summary>
/// #3933 against a store: the fenced scan returns exactly the buckets the old text returned, on every kind of
/// target the pass walks, and its plan runs each probe per bucket instead of as a join.
///
/// <para><b>The oracle is the old text.</b> <see cref="OldScanSql"/> is the scan as it stood before #3933, so the
/// two differ in exactly the fences. The seed is one two-day timeline that plants every case the probes decide: a
/// tail collected and never materialized (the holes), an outage with nothing collected (not holes, but the buckets
/// that sent the old source probe through its whole <c>Materialize</c>), an hour holding only a restart row (a hole
/// for an aggregate that admits the row, not for one whose own <c>WHERE</c> rejects it), covered hours, and a day
/// the hierarchical daily never materialized while its hourly source holds it.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class MaterializationHoleScanShapeLiveTests
{
    private const int ServerId = -393301;
    private const string ServerName = "hole-scan-shape-3933";

    /// <summary>The pre-#3933 scan, verbatim.</summary>
    private static string OldScanSql(TimescaleSupport.MaterializationHoleTarget target, (string Schema, string Name) materialization)
    {
        var filter = TimescaleSupport.MaterializationHoleSourceFilterFor(target.CreateSql);
        var sourceFilter = filter.Length == 0 ? string.Empty : $"\n    AND   {filter}";

        return $@"
SELECT b.bucket
FROM generate_series($1::timestamp, $2::timestamp, $3::interval) AS b(bucket)
WHERE NOT EXISTS (SELECT 1 FROM ""{materialization.Schema}"".""{materialization.Name}"" AS m WHERE m.bucket = b.bucket)
AND   EXISTS (
    SELECT 1 FROM collect.{target.Source} AS s
    WHERE s.{target.SourceTimeColumn} >= b.bucket
    AND   s.{target.SourceTimeColumn} < b.bucket + $3::interval{sourceFilter})
ORDER BY b.bucket";
    }

    [Fact]
    public async Task TheFencedScan_FindsExactlyTheOldScansBuckets_OnEveryKindOfTarget_AgainstDevPostgres()
    {
        await using var seed = await SeedAsync("the hole-scan oracle test");
        var (connection, h0) = (seed.Connection, seed.H0);
        var ct = TestContext.Current.CancellationToken;
        DateTime H(int n) => h0.AddHours(n);

        /* #3653 LC: query_stats_hourly and query_stats_daily are frozen out of MaterializationHoleTargets — the
           repair walk (and this oracle, which reads the same registry) never scans them any more. The legacy's
           own half of the restart-row contrast (it admits H20, so H20 is neither a hole nor absent) is pinned
           purely against its CreateSql text (MaterializationHoleRepairTests.TheSourceFilter_...); what remains
           live to prove is the successor's half. */
        var expected = new Dictionary<string, DateTime[]>(StringComparer.Ordinal)
        {
            /* The successor rejects the restart row, so H20 is neither materialized nor a hole for it. */
            [TimescaleSupport.QueryStatsIntervalHourlyView] = new[] { H(3), H(4) },
            [TimescaleSupport.QueryStatsBaselineView] = new[] { H(3), H(4) },
        };

        foreach (var (view, holes) in expected)
        {
            var target = TimescaleSupport.MaterializationHoleTargets.Single(t => t.View == view);
            var materialization = await TimescaleSupport.ResolveMaterializationAsync(connection, view, ct);
            Assert.NotNull(materialization);
            var to = target.BucketWidth >= TimescaleSupport.DailyBucket ? h0.AddDays(1) : H(47);

            var current = await ScanAsync(connection, TimescaleSupport.MaterializationHoleScanSql(target, materialization.Value), h0, to, target.BucketWidth, ct);
            var oracle = await ScanAsync(connection, OldScanSql(target, materialization.Value), h0, to, target.BucketWidth, ct);

            Assert.Equal(oracle, current);
            Assert.Equal(holes, current);
        }
    }

    /// <summary>
    /// The plan, not the text: nothing materialized and no semi-, anti- or merge join in the scan, which is how
    /// the old probes ran. The materialization probe runs once per bucket of the span (48), and the source
    /// probe once per bucket the materialization probe found empty: the two tail hours and the four outage
    /// hours, six.
    /// </summary>
    [Fact]
    public async Task TheScanProbesPerBucket_AndTheSourceOnlyWhereTheMaterializationIsEmpty_AgainstDevPostgres()
    {
        await using var seed = await SeedAsync("the hole-scan plan test");
        var (connection, h0) = (seed.Connection, seed.H0);
        var ct = TestContext.Current.CancellationToken;

        /* #3653 LC: query_stats_hourly is frozen out of MaterializationHoleTargets; the live successor is
           refreshed over the identical windows in SeedAsync, so its plan shape is the same proof. */
        var target = TimescaleSupport.MaterializationHoleTargets.Single(t => t.View == TimescaleSupport.QueryStatsIntervalHourlyView);
        var materialization = await TimescaleSupport.ResolveMaterializationAsync(connection, target.View, ct);
        Assert.NotNull(materialization);

        await using var explain = new NpgsqlCommand(
            "EXPLAIN (ANALYZE, FORMAT JSON) " + TimescaleSupport.MaterializationHoleScanSql(target, materialization.Value), connection);
        explain.Parameters.AddWithValue(h0);
        explain.Parameters.AddWithValue(h0.AddHours(47));
        explain.Parameters.AddWithValue(target.BucketWidth);
        var json = (string)(await explain.ExecuteScalarAsync(ct))!;
        using var plan = JsonDocument.Parse(json);

        var nodes = Walk(plan.RootElement[0].GetProperty("Plan")).ToArray();
        Assert.DoesNotContain(nodes, n => NodeType(n) == "Materialize");
        Assert.DoesNotContain(nodes, n => NodeType(n) is "Nested Loop" or "Merge Join" or "Hash Join");

        var buckets = nodes.Single(n => NodeType(n) == "Function Scan"
            && n.TryGetProperty("Function Name", out var fn) && fn.GetString() == "generate_series");
        Assert.Equal(48, LoopsOfTheOnlySubPlanUnder(buckets));

        var candidates = nodes.Single(n => NodeType(n) == "Subquery Scan" && n.GetProperty("Alias").GetString()!.StartsWith('c'));
        /* #3653 LC: the test switched from query_stats_hourly (legacy, which admitted the H20 restart row and
           created a bucket for it) to query_stats_interval_hourly (successor, which filters restart rows in its
           WHERE and therefore creates NO bucket for H20). H20 is now a candidate (missing from the aggregate)
           even though the source check rejects it (restart row filtered). Candidates = H3, H4, H5, H6, H7, H8,
           H20 = 7. The "source only where the materialization is empty" shape is unchanged: the outer EXISTS
           filters H5-H8 (no raw data) and H20 (only a restart row, filtered by the source filter), leaving H3
           and H4 as the actual holes repaired. */
        Assert.Equal(7, LoopsOfTheOnlySubPlanUnder(candidates));
    }

    /* ───────────────────────────── seed ───────────────────────────── */

    private sealed class Seeded(ScratchPostgres scratch, NpgsqlConnection connection, DateTime h0) : IAsyncDisposable
    {
        public NpgsqlConnection Connection { get; } = connection;

        public DateTime H0 { get; } = h0;

        public async ValueTask DisposeAsync()
        {
            await Connection.DisposeAsync();
            await scratch.DisposeAsync();
        }
    }

    /// <summary>
    /// Two whole UTC days, H0 at midnight six days back: below every refresh policy's window, so a background run
    /// cannot materialize what the seed leaves unmaterialized (the scan is called with explicit bounds, so no
    /// retention horizon is involved). Day A collects every hour but H5-H8 (the outage), and H20 holds only a restart row (interval 0,
    /// no execution). Day B collects every hour. Refreshed: the legacy hourly, the successor and the query
    /// baseline over [H0, H3) and [H9, H48), so H3 and H4 are the tail; the daily over day A only.
    /// </summary>
    private static async Task<Seeded> SeedAsync(string what)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            $"Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run {what}.");
        var ct = TestContext.Current.CancellationToken;

        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        try
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.SkipWhen(!await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct),
                $"{what} needs TimescaleDB: a materialization hole exists only in a materialization.");

            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

            var h0 = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-6), DateTimeKind.Unspecified);
            Assert.True(DateTime.UtcNow - h0.AddHours(48) > TimescaleSupport.DailyRefreshStartSpan, "the seed must sit below every refresh policy's window");

            var collected = Enumerable.Range(0, 48).Where(h => h is not (>= 5 and <= 8) and not 20).ToArray();
            await PlantHoursAsync(connection, h0, collected, ct);
            await using (var restart = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES (-1, $1, $2, $3, 'ScanDb', '0xRESTART', '0xRESTARTHANDLE', 0, 0, 0, 0)", connection))
            {
                restart.Parameters.AddWithValue(h0.AddHours(20).AddMinutes(10));
                restart.Parameters.AddWithValue(ServerId);
                restart.Parameters.AddWithValue(ServerName);
                await restart.ExecuteNonQueryAsync(ct);
            }

            foreach (var view in new[] { TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsIntervalHourlyView, TimescaleSupport.QueryStatsBaselineView })
            {
                await RefreshAsync(connection, view, h0, h0.AddHours(3), ct);
                await RefreshAsync(connection, view, h0.AddHours(9), h0.AddHours(48), ct);
            }

            await RefreshAsync(connection, TimescaleSupport.QueryStatsDailyView, h0, h0.AddDays(1), ct);
            return new Seeded(scratch, connection, h0);
        }
        catch
        {
            /* A skip is an exception too: the scratch database goes with it either way. */
            await connection.DisposeAsync();
            await scratch.DisposeAsync();
            throw;
        }
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

    private static async Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
    {
        /* The plain, unforced form a policy runs — the CALL cannot be inside a transaction. */
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(from);
        refresh.Parameters.AddWithValue(to);
        await refresh.ExecuteNonQueryAsync(ct);
    }

    private static async Task<DateTime[]> ScanAsync(NpgsqlConnection connection, string sql, DateTime from, DateTime to, TimeSpan width, CancellationToken ct)
    {
        var buckets = new List<DateTime>();
        await using var scan = new NpgsqlCommand(sql, connection);
        scan.Parameters.AddWithValue(from);
        scan.Parameters.AddWithValue(to);
        scan.Parameters.AddWithValue(width);
        await using var reader = await scan.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            buckets.Add(reader.GetDateTime(0));
        }

        return buckets.ToArray();
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

    /// <summary>How many times the one SubPlan hanging off <paramref name="node"/> ran: the probe's per-row
    /// execution count, which is what tells a per-bucket probe from a join.</summary>
    private static int LoopsOfTheOnlySubPlanUnder(JsonElement node)
    {
        var subPlans = node.GetProperty("Plans").EnumerateArray()
            .Where(child => child.GetProperty("Parent Relationship").GetString() == "SubPlan")
            .ToArray();
        Assert.Single(subPlans);
        return subPlans[0].GetProperty("Actual Loops").GetInt32();
    }
}
