/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3691 W1, live: <c>compare_analysis</c> on a PostgreSQL target sigma-bands <c>PG_TPS</c> against the server's own
/// <c>pg_tps</c> hour-of-week bucket. The v1 exit check ran exactly this scenario — 31 days at ~10 tps, the recent
/// four hours at 60 — and the tool banded the family "stable" (<c>relative_move 0.83</c>, <c>band_source: absolute</c>,
/// <c>baseline_metric: null</c>) in the same window the anomaly detector called 25σ, because
/// <c>ComparisonBanding.BaselinedMetricFor</c> mapped only the SQL Server keys and <c>PG_TPS</c>'s ladder position is
/// 0 by design. The arithmetic is pinned ungated in <c>ComparisonBandingTests</c> (Lite.Tests); this class proves the
/// WIRING end to end through the REAL tool: <c>DarlingAnalysisService.ComparePeriodsAsync</c> resolves the engine per
/// call from <c>servers.engine_kind</c>, asks THAT engine's baseline provider (<see cref="PgTargetBaselineProvider"/>)
/// for the metrics <c>DispersionMetricsFor</c> names — which now include <c>pg_tps</c> — anchored at the comparison
/// window's start exactly as the detector anchors, and the row comes back <c>worse</c> by sigma with the bucket's
/// figures beside it. W2 rides along: the payload's <c>plan_cache_churn.note</c> is the PostgreSQL sentence.
///
/// <para>Gated on <c>DARLING_TEST_PG</c>; <c>[Collection("live-postgres")]</c> (#1776) and the #1902 cleanup shape.
/// The planted series is the anomaly e2e's (<c>PgTargetAnomalyTests</c>) with only <c>pg_database_stats</c>, which is
/// also the coverage witness, so both windows read fully observed and no coverage caveat rides on the verdict.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class CompareAnalysisPgBandingLiveTests
{
    private const string ServerName = "darling-pg-target-compare-banding-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    [Fact]
    public async Task ASixFoldTpsSurge_IsBandedWorseBySigma_AgainstThePgTpsBucket_ThroughTheRealTool()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the compare_analysis banding e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, ServerId, ServerName, MonitoredEngineKind.Postgres, 18, ct);

            /* 31 days of one-minute samples ending a minute ago. Steady state ~10 tps with a deterministic ±1 ripple
               so the bucket's MAD is small but non-zero; the last 250 minutes run at 60 tps so the tool's default
               four-hour comparison window (anchored at now) sits wholly inside the surge and its default baseline
               window (28 h back, same duration) sits wholly in the steady state. */
            var end = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            const int minutes = 31 * 24 * 60;
            var start = end.AddMinutes(-minutes);
            const int surgeFrom = minutes - 250;

            using (var plant = new NpgsqlCommand(@"
WITH s AS (
    SELECT n, CASE WHEN n >= $5 THEN 3600 ELSE 600 + round(60 * sin(n)) END AS commits
    FROM generate_series(0, $6) AS n
)
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
SELECT $1 + n, $2 + (n * interval '1 minute'), $3, $4, 'appdb',
       SUM(commits) OVER (ORDER BY n), 0, 100, 9000, 0, 0, 0, NULL
FROM s", connection) { CommandTimeout = 120 })
            {
                plant.Parameters.AddWithValue(CollectionIdGenerator.Next() + 1_000_000L);
                plant.Parameters.AddWithValue(start);
                plant.Parameters.AddWithValue(ServerId);
                plant.Parameters.AddWithValue(ServerName);
                plant.Parameters.AddWithValue(surgeFrom);
                plant.Parameters.AddWithValue(minutes);
                await plant.ExecuteNonQueryAsync(ct);
            }

            /* The bucket the tool will read: trustworthy, centred on the steady state, tight. */
            var bucket = await new PgTargetBaselineProvider(postgres).GetBaselineAsync(ServerId, MetricNames.PgTps, end.AddHours(-4), ct);
            Assert.True(bucket.IsTrustworthy, "the 31-day pg_tps bucket is not trustworthy");
            Assert.InRange(bucket.Median, 9.0, 11.0);
            Assert.True(bucket.EffectiveRobustSigma > 0);

            var service = new DarlingAnalysisService(postgres);
            var json = await DarlingMcpTools.CompareAnalysis(service, postgres, ServerName, hours_back: 4, baseline_hours_back: 28);
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            Assert.True(root.TryGetProperty("facts", out var facts), $"compare_analysis returned no verdict rows:\n{json}");

            var tps = Assert.Single(facts.EnumerateArray(), f => f.GetProperty("key").GetString() == PgTargetFactKeys.Tps);
            Assert.InRange(tps.GetProperty("baseline_value").GetDouble(), 9.0, 11.0);
            Assert.InRange(tps.GetProperty("comparison_value").GetDouble(), 59.0, 61.0);
            Assert.Equal(ComparisonBanding.StatusWorse, tps.GetProperty("status").GetString());
            Assert.Equal(ComparisonBanding.BandSourceBaseline, tps.GetProperty("band_source").GetString());
            Assert.Equal(MetricNames.PgTps, tps.GetProperty("baseline_metric").GetString());
            Assert.True(tps.GetProperty("beyond_anomaly_cutoff").GetBoolean());
            Assert.Equal(AnomalyThresholds.SigmaDisplayCap, tps.GetProperty("delta_sigma").GetDouble());
            Assert.InRange(tps.GetProperty("baseline_median").GetDouble(), 9.0, 11.0);
            Assert.Equal(0.0, tps.GetProperty("ladder_position").GetDouble()); // the ladder that could never have moved it
            Assert.False(tps.GetProperty("coverage_caveat").GetBoolean());

            var family = Assert.Single(root.GetProperty("families").EnumerateArray(), f => f.GetProperty("family").GetString() == PgTargetFactKeys.Tps);
            Assert.Equal(ComparisonBanding.StatusWorse, family.GetProperty("status").GetString());
            Assert.True(root.GetProperty("summary").GetProperty("families_worse").GetInt32() >= 1);

            Assert.Equal(PlanCacheChurn.PostgresNote, root.GetProperty("plan_cache_churn").GetProperty("note").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static DateTime TruncateToMinutes(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_database_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
