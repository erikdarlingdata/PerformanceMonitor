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
/// The object-growth family's gated e2e (#3691 lane 38): a planted fortnight of hourly <c>pg_database_size_stats</c>
/// for three databases — <c>appdb</c> growing 100 → 140 GB, <c>reporting</c> flat at 24 GB, <c>scratch</c> flat at
/// 2 GB — plus an unsized <c>vendor</c> database (NULL <c>size_bytes</c>, so the instance total is NULL on every row
/// exactly as the collector writes it), and a five-day-old row far above the lookback as a trap. Through the REAL
/// collector: one <c>PG_DATABASE_GROWTH</c> naming <c>appdb</c> with 40 GB / 40 % / ~2.9 GB a day and the
/// straight-line days-to-double, the unsized database counted and the total marked unavailable; through the REAL
/// <c>analyze_server</c>: a finding rooted on it whose headline names the database and the doubling estimate, with
/// PostgreSQL <c>next_tools</c>, and <c>get_analysis_facts source=pg_growth</c> showing it with
/// <c>threshold_lineage = 0</c>. The growth-rate anomaly is ALSO exercised on the live store: its baseline arm and
/// window read run against the planted series (the total is NULL here by design, so the detector is silent — the
/// brief's "silent where the total is NULL" arm, asserted), and a second planting with a sized instance proves the
/// SQL of both reads executes and the arm yields buckets. Gated on <c>DARLING_TEST_PG</c>; <c>[Collection("live-postgres")]</c>
/// + <c>LiveStoreCleanup.RunAsync</c> (the #1902 ratchet).
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetGrowthLiveTests
{
    private const string ServerName = "darling-pg-target-growth-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const long MiB = 1024L * 1024;
    private const long GiB = 1024L * MiB;

    [Fact]
    public async Task APlantedFortnightOfSizes_ProducesOneTrendNamingTheGrowingDatabase_WithItsDoublingEstimate_ThroughAnalyzeServer()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the growth-family e2e.");

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

            /* Whole-minute bounds, window ending a minute ago (the plumbing e2e's reasoning). */
            var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var windowStart = windowEnd.AddHours(-4);
            var lookbackStart = windowEnd.AddDays(-PgTargetScorer.GrowthLookbackDays);

            /* The gate and the coverage witness: 25 h of span, one row a minute across the window. */
            await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, ServerId, ServerName, windowEnd.AddHours(-25), ct);
            for (var minute = 0; minute <= 4 * 60 + 1; minute++)
                await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, ServerId, ServerName, windowStart.AddMinutes(minute - 1), ct);

            /* ── pg_database_size_stats, hourly across the lookback: 336 samples, hour 1 .. hour 336, the last at the
               window end. appdb 100 → 140 GB linear; reporting flat 24 GB; scratch flat 2 GB; vendor NULL (unsized) on
               every row, which NULLs total_bytes on every row — the collector's own rule. A trap row a day BEFORE the
               lookback at 500 GB must not be the earliest. */
            const int hours = 14 * 24;
            await PlantHourAsync(connection, lookbackStart.AddDays(-1), appdb: 500 * GiB, ct);
            for (var h = 1; h <= hours; h++)
            {
                var appdb = 100 * GiB + (long)((h - 1) / (double)(hours - 1) * 40 * GiB);
                await PlantHourAsync(connection, lookbackStart.AddHours(h), appdb, ct);
            }

            /* ── the collector alone. */
            var collector = new PgTargetFactCollector(postgres);
            var context = new AnalysisContext
            {
                ServerId = ServerId,
                ServerName = ServerName,
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero,
            };
            var facts = await collector.CollectFactsAsync(context);
            Assert.False(context.Coverage!.IsPartial);

            var trend = Assert.Single(facts, f => f.Key == PgTargetFactKeys.DatabaseGrowth);
            Assert.Equal(PgTargetSources.GrowthSource, trend.Source);
            Assert.Equal("appdb", trend.ObjectName);
            Assert.Equal("appdb", trend.DatabaseName);
            Assert.Equal(40 * GiB, trend.Value);
            Assert.Equal(1, trend.Metadata[PgTargetScorer.GrowthAvailableKey]);
            /* The earliest sample inside the lookback is hour 1 (100 GB) — not the 500 GB trap a day before it. */
            Assert.Equal(100 * GiB, trend.Metadata[PgTargetScorer.GrowthFirstBytesKey]);
            Assert.Equal(140 * GiB, trend.Metadata[PgTargetScorer.GrowthLatestBytesKey]);
            Assert.Equal(40.0, trend.Metadata[PgTargetScorer.GrowthPctKey], precision: 6);
            Assert.Equal(1, trend.Metadata[PgTargetScorer.GrowthPctComputableKey]);
            Assert.Equal(hours, trend.Metadata[PgTargetScorer.GrowthSamplesKey]);
            Assert.Equal(hours - 1, trend.Metadata[PgTargetScorer.GrowthSpanHoursKey], precision: 6);
            Assert.Equal(hours, trend.Metadata[PgTargetScorer.GrowthSamplesInLookbackKey]);
            Assert.Equal(PgTargetScorer.GrowthLookbackDays, trend.Metadata[PgTargetScorer.GrowthLookbackDaysKey]);
            /* The slope and its straight line: 40 GiB over 335 h = 2.866 GiB/day; 140 GiB at that rate doubles in 48.85 days. */
            var perDay = 40.0 * GiB / (335.0 / 24.0);
            Assert.Equal(perDay, trend.Metadata[PgTargetScorer.GrowthBytesPerDayKey], precision: 3);
            Assert.Equal(140.0 * GiB / perDay, trend.Metadata[PgTargetScorer.GrowthDaysToDoubleKey], precision: 6);
            Assert.Equal(40.0 / (335.0 / 24.0), trend.Metadata[PgTargetScorer.GrowthPctPerDayKey], precision: 6);
            /* Population: four databases seen, ONE unsized, three with samples, one over the line. The total is NULL on
               every row because vendor is unsized — unavailable, never a sum over the three the role can see. */
            Assert.Equal(4, trend.Metadata[PgTargetScorer.GrowthDatabasesSeenKey]);
            Assert.Equal(1, trend.Metadata[PgTargetScorer.GrowthDatabasesUnsizedKey]);
            Assert.Equal(3, trend.Metadata[PgTargetScorer.GrowthDatabasesConsideredKey]);
            Assert.Equal(1, trend.Metadata[PgTargetScorer.GrowthDatabasesOverLineKey]);
            Assert.Equal(0, trend.Metadata[PgTargetScorer.GrowthTotalAvailableKey]);
            Assert.Equal(0, trend.Metadata[PgTargetScorer.GrowthTotalSamplesKey]);
            Assert.False(trend.Metadata.ContainsKey(PgTargetScorer.GrowthTotalBytesKey));
            /* The three sized databases ride by name (growth 0 for the flat two); the unsized one is NOT named. */
            Assert.Equal(["appdb", "reporting", "scratch"], PgTargetScorer.GrowthNamedDatabases(trend).Order(StringComparer.Ordinal).ToList());
            Assert.Equal(0, trend.Metadata[PgTargetScorer.GrowthNamedBytesPrefix + "reporting"]);
            Assert.DoesNotContain(trend.Metadata.Keys, k => k.Contains("vendor", StringComparison.Ordinal));

            /* Scored: 40 GiB is past the bytes critical (1.0); 40 % is past the fraction critical (1.0) → 1.0, database
               subject, lineage 0. */
            new FactScorer().ScoreAll(facts);
            Assert.Equal(1.0, trend.BaseSeverity, precision: 9);
            Assert.Equal(0, trend.Metadata["threshold_lineage"]);
            Assert.Equal(PgTargetScorer.GrowthSubjectDatabase, trend.Metadata[PgTargetScorer.GrowthGradedSubjectKey]);

            /* ── the anomaly side on the live store: the total is NULL on every row, so the baseline arm yields NO
               buckets and the detector is silent by design — asserted, not assumed. */
            var baselines = new PgTargetBaselineProvider(postgres);
            var noTotal = await baselines.GetBaselineAsync(ServerId, MetricNames.PgDatabaseGrowthBytesPerDay, windowStart, ct);
            Assert.Equal(0, noTotal.SampleCount);
            var detector = new PgTargetAnomalyDetector(postgres, baselines);
            Assert.DoesNotContain(await detector.DetectAnomaliesAsync(context), a => a.Key == PgTargetFactKeys.AnomalyDatabaseGrowth);

            /* ── THE EXIT CRITERION: the real analyze_server returns PG_DATABASE_GROWTH naming appdb with the days-to-double estimate. */
            var service = new DarlingAnalysisService(postgres);
            var analysis = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, 4);
            using (var doc = JsonDocument.Parse(analysis))
            {
                var root = doc.RootElement;
                Assert.Equal("findings", root.GetProperty("status").GetString());
                var findings = root.GetProperty("findings").EnumerateArray().ToList();
                var finding = Assert.Single(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.DatabaseGrowth);
                Assert.Equal(PgTargetSources.GrowthSource, finding.GetProperty("category").GetString());
                Assert.Equal(40.0 * GiB, finding.GetProperty("root_fact").GetProperty("value").GetDouble(), precision: 3);
                var advice = finding.GetProperty("advice");
                Assert.Equal("appdb grew 40 GB (40%) in 14 days — 2.9 GB/day; at that slope it doubles in ~49 days", advice.GetProperty("headline").GetString());
                var investigation = advice.GetProperty("investigation").GetString()!;
                Assert.Contains("a straight-line extrapolation of the lookback's slope, not a forecast", investigation, StringComparison.Ordinal);
                Assert.Contains("The instance total could not be trended: 1 database could not be sized by the monitoring role", investigation, StringComparison.Ordinal);
                Assert.Contains("Population: 4 databases seen in the lookback, 1 unsized, 3 with at least 3 samples, 1 over the line.", investigation, StringComparison.Ordinal);
                var remediation = advice.GetProperty("remediation").GetString()!;
                Assert.StartsWith("Ask the bloat question first.", remediation, StringComparison.Ordinal);
                Assert.Contains("Disk free space is NOT collected for a PostgreSQL target", remediation, StringComparison.Ordinal);
                Assert.DoesNotContain("CREATE INDEX", remediation, StringComparison.OrdinalIgnoreCase);
                /* PostgreSQL next_tools, never a SQL Server one. */
                var tools = finding.GetProperty("next_tools").EnumerateArray().Select(t => t.GetProperty("tool").GetString()!).ToList();
                Assert.Contains("get_analysis_facts", tools);
                Assert.Contains("get_pg_table_bloat", tools);
                Assert.DoesNotContain(tools, t => t.StartsWith("get_wait", StringComparison.Ordinal) || t.StartsWith("get_index", StringComparison.Ordinal) || t == "get_database_sizes");
            }

            /* The facts read shows the family under its source with the unmeasured-lineage flag. */
            var factsJson = await DarlingMcpTools.GetAnalysisFacts(service, postgres, ServerName, 4, PgTargetSources.GrowthSource);
            using (var doc = JsonDocument.Parse(factsJson))
            {
                var root = doc.RootElement;
                Assert.Equal(1, root.GetProperty("shown").GetInt32());
                var fact = Assert.Single(root.GetProperty("facts").EnumerateArray());
                Assert.Equal(PgTargetFactKeys.DatabaseGrowth, fact.GetProperty("key").GetString());
                Assert.Equal(0, fact.GetProperty("metadata").GetProperty("threshold_lineage").GetDouble());
            }

            /* ── a SIZED instance: re-plant with vendor sized too (total = the four), so the total's trend exists and
               the anomaly's baseline arm and window read both execute against the real table. The baseline holds a
               steady ~2.9 GiB/day over the fortnight (hour-only tier at best on 14 days); the window's last hour is
               the same slope, so the detector reads buckets and stays quiet — the SQL of both reads is proven, the
               zero-delta shape is not what fires. */
            await DeleteSizeRowsAsync(connection, ct);
            for (var h = 1; h <= hours; h++)
            {
                var appdb = 100 * GiB + (long)((h - 1) / (double)(hours - 1) * 40 * GiB);
                await PlantHourAsync(connection, lookbackStart.AddHours(h), appdb, ct, vendorSized: true);
            }
            var sizedFacts = await collector.CollectFactsAsync(context);
            var sized = Assert.Single(sizedFacts, f => f.Key == PgTargetFactKeys.DatabaseGrowth);
            Assert.Equal(1, sized.Metadata[PgTargetScorer.GrowthTotalAvailableKey]);
            Assert.Equal(hours, sized.Metadata[PgTargetScorer.GrowthTotalSamplesKey]);
            Assert.Equal(40 * GiB, sized.Metadata[PgTargetScorer.GrowthTotalBytesKey]);
            Assert.Equal((100 + 24 + 2 + 1) * GiB, sized.Metadata[PgTargetScorer.GrowthTotalFirstBytesKey]);
            Assert.Equal((140 + 24 + 2 + 1) * GiB, sized.Metadata[PgTargetScorer.GrowthTotalLatestBytesKey]);
            Assert.Equal(0, sized.Metadata[PgTargetScorer.GrowthDatabasesUnsizedKey]);
            Assert.Equal(4, sized.Metadata[PgTargetScorer.GrowthDatabasesConsideredKey]);
            Assert.Equal(["appdb", "reporting", "scratch"], PgTargetScorer.GrowthNamedDatabases(sized).Order(StringComparer.Ordinal).ToList());   /* top 3 by growth: vendor (0) ties and sorts last by size */

            var withTotal = await new PgTargetBaselineProvider(postgres).GetBaselineAsync(ServerId, MetricNames.PgDatabaseGrowthBytesPerDay, windowStart, ct);
            Assert.True(withTotal.SampleCount > 0, "the baseline arm must yield buckets over a sized instance");
            Assert.InRange(withTotal.Mean, perDay * 0.9, perDay * 1.1);
            var anomalies = await new PgTargetAnomalyDetector(postgres, new PgTargetBaselineProvider(postgres)).DetectAnomaliesAsync(context);
            Assert.DoesNotContain(anomalies, a => a.Key == PgTargetFactKeys.AnomalyDatabaseGrowth);

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

    /// <summary>One collection's four rows, the collector's shape: every database, the total denormalised onto each row
    /// and NULL on every row when any database is unsized (<paramref name="vendorSized"/> false).</summary>
    private static async Task PlantHourAsync(NpgsqlConnection connection, DateTime at, long appdb, CancellationToken ct, bool vendorSized = false)
    {
        var collectionId = CollectionIdGenerator.Next();
        long? vendor = vendorSized ? GiB : null;
        long? total = vendorSized ? appdb + 24 * GiB + 2 * GiB + GiB : null;
        foreach (var (name, size) in new (string, long?)[] { ("appdb", appdb), ("reporting", 24 * GiB), ("scratch", 2 * GiB), ("vendor", vendor) })
        {
            using var command = new NpgsqlCommand(@"
INSERT INTO pg_database_size_stats
    (collection_id, collection_time, server_id, server_name, database_name, size_bytes, total_bytes, is_template, allows_connections)
VALUES ($1, $2, $3, $4, $5, $6, $7, FALSE, TRUE)", connection);
            command.Parameters.AddWithValue(collectionId);
            command.Parameters.AddWithValue(at);
            command.Parameters.AddWithValue(ServerId);
            command.Parameters.AddWithValue(ServerName);
            command.Parameters.AddWithValue(name);
            command.Parameters.Add(new NpgsqlParameter { Value = size.HasValue ? size.Value : DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bigint });
            command.Parameters.Add(new NpgsqlParameter { Value = total.HasValue ? total.Value : DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bigint });
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task DeleteSizeRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM pg_database_size_stats WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_database_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM pg_database_size_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
