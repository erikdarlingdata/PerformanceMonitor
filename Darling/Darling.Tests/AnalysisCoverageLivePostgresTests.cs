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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3538 A2 on the real store (gated, DARLING_TEST_PG): every rate and fraction fact divides by the time
/// the collector actually observed, a window it only partly observed says so in every analysis payload,
/// and a fully collected window is unchanged. Lite's <c>FactCollectorTests</c> /
/// <c>AnalysisCoverageTests</c> are the twin; this is the same scenario through Darling's collector,
/// service and tools, because the coverage witness is a QUERY and a query is proven on the engine that
/// runs it.
///
/// <para>The scenario is the review's failure case: a CXPACKET storm at a true 25% of observed time and
/// ten blocking events, with the collector down for three of the window's four hours. Divided by the
/// nominal window that read as 6% and 2.5/hr — under every bar — with no caveat. The series is planted
/// as a collector would have written it: a baseline reading at the window start whose delta is
/// unknowable, then a delta every fifteen minutes for the hour the collector was up, and nothing after.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class AnalysisCoverageLivePostgresTests
{
    private const string ServerName = "darling-analysis-coverage-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task APartlyCollectedWindow_RatesPerObservedTime_AndEveryPayloadSaysSo()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live coverage test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(connection, ct);

            /* Whole-minute bounds so the PG microsecond comparisons are exact; the window ends a minute
               ago so "now" inside the tools is safely past every planted row. */
            var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var windowStart = windowEnd.AddHours(-4);

            /* The data-span gate measures lifetime history: one old row carries the server past it. */
            await PlantWaitAsync(connection, windowEnd.AddHours(-30), "OLD_WAIT", 1_000L, ct);

            /* The first hour: a baseline reading, then four deltas of 225,000 ms = 900,000 ms of
               CXPACKET over ONE observed hour. 0.25 of observed time; 0.0625 of the nominal window. */
            await PlantWaitAsync(connection, windowStart, "CXPACKET", 0L, ct);
            for (var i = 1; i <= 4; i++)
                await PlantWaitAsync(connection, windowStart.AddMinutes(15 * i), "CXPACKET", 225_000L, ct);

            /* Ten blocking events in that hour: 10/hr observed, 2.5/hr nominal. */
            for (var i = 0; i < 10; i++)
                await PlantBlockingAsync(connection, windowStart.AddMinutes(5 + i * 5), 70 + i, ct);

            var collector = new PgFactCollector(postgres);
            var context = new AnalysisContext
            {
                ServerId = ServerId,
                ServerName = ServerName,
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero
            };
            var facts = await collector.CollectFactsAsync(context);

            /* ── the stamp: one hour of four, the three-hour tail the largest hole. */
            var coverage = context.Coverage!;
            Assert.True(coverage.IsPartial);
            Assert.Equal(0.25, coverage.Fraction, precision: 6);
            Assert.Equal(3_600_000, coverage.ObservedMs, precision: 3);
            Assert.Equal(10_800_000, coverage.LargestGapMs, precision: 3);
            Assert.Equal(5, coverage.SampleCount);

            /* ── the facts: per observed time, four times what the nominal division claimed. */
            var cx = Assert.Single(facts, f => f.Key == "CXPACKET");
            Assert.Equal(0.25, cx.Value, precision: 6);
            Assert.Equal(0.25, cx.Metadata["coverage_fraction"], precision: 6);
            Assert.Equal(14_400_000, cx.Metadata["period_duration_ms"]);
            Assert.Equal(cx.Value, cx.Metadata["wait_time_ms"] / (cx.Metadata["period_duration_ms"] * cx.Metadata["coverage_fraction"]), precision: 6);

            var blocking = Assert.Single(facts, f => f.Key == "BLOCKING_EVENTS");
            Assert.Equal(10.0, blocking.Value, precision: 6);
            Assert.Equal(1.0, blocking.Metadata["observed_hours"], precision: 6);
            Assert.Equal(4.0, blocking.Metadata["period_hours"], precision: 6);

            var gap = Assert.Single(facts, f => f.Key == WindowCoverage.FactKey);
            Assert.Equal(WindowCoverage.FactSource, gap.Source);
            Assert.Equal(0.25, gap.Value, precision: 6);
            Assert.Equal(10_800_000, gap.Metadata["largest_gap_ms"], precision: 3);

            /* ── the tools. The window inside them ends at "now", a minute past windowEnd, so the
               figures are a hair under the collector-level ones; the claims are the same. */
            var service = new DarlingAnalysisService(postgres);

            var analysis = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, 4);
            using (var doc = JsonDocument.Parse(analysis))
            {
                var root = doc.RootElement;
                Assert.Equal("findings", root.GetProperty("status").GetString());
                Assert.Contains("PARTIAL COVERAGE", root.GetProperty("caveat").GetString()!, StringComparison.Ordinal);
                var cov = root.GetProperty("coverage");
                Assert.True(cov.GetProperty("partial").GetBoolean());
                Assert.InRange(cov.GetProperty("observed_fraction").GetDouble(), 0.24, 0.26);
                Assert.InRange(cov.GetProperty("largest_gap_hours").GetDouble(), 2.99, 3.05);

                var cxFinding = root.GetProperty("findings").EnumerateArray()
                    .Single(f => f.GetProperty("root_fact").GetProperty("key").GetString() == "CXPACKET");
                Assert.InRange(cxFinding.GetProperty("root_fact").GetProperty("value").GetDouble(), 0.24, 0.27);
            }
            Assert.True(service.LastWindowCoverage is { IsPartial: true });

            var factsPayload = await DarlingMcpTools.GetAnalysisFacts(service, postgres, ServerName, 4);
            using (var doc = JsonDocument.Parse(factsPayload))
            {
                var root = doc.RootElement;
                Assert.Contains("PARTIAL COVERAGE", root.GetProperty("caveat").GetString()!, StringComparison.Ordinal);
                Assert.True(root.GetProperty("coverage").GetProperty("partial").GetBoolean());
                Assert.Contains(root.GetProperty("facts").EnumerateArray(),
                    f => f.GetProperty("key").GetString() == WindowCoverage.FactKey);
            }

            /* compare_analysis: the baseline window (28h back) was never observed, the comparison
               window partly — both caveats, and the gap fact reported through the coverage blocks
               rather than as a compared key. */
            var compared = await DarlingMcpTools.CompareAnalysis(service, postgres, ServerName, 4, 28);
            using (var doc = JsonDocument.Parse(compared))
            {
                var root = doc.RootElement;
                var caveat = root.GetProperty("caveat").GetString()!;
                Assert.Contains("The BASELINE window produced no facts at all", caveat, StringComparison.Ordinal);
                Assert.Contains("The COMPARISON window was only partly collected", caveat, StringComparison.Ordinal);
                Assert.True(root.GetProperty("baseline").GetProperty("coverage").GetProperty("unobserved").GetBoolean());
                Assert.True(root.GetProperty("comparison").GetProperty("coverage").GetProperty("partial").GetBoolean());
                Assert.DoesNotContain(root.GetProperty("facts").EnumerateArray(),
                    f => f.GetProperty("key").GetString() == WindowCoverage.FactKey);

                /* #3538 A3 composes with the caveat: every verdict row and family carries coverage_caveat,
                   the storm is a comparison-only row banded by presence (worse: 0.25 saturates CXPACKET's
                   ladder), the rules are stated, and the summary counts families beside rows. */
                Assert.True(root.GetProperty("summary").GetProperty("coverage_caveat").GetBoolean());
                Assert.All(root.GetProperty("facts").EnumerateArray(), f => Assert.True(f.GetProperty("coverage_caveat").GetBoolean()));
                Assert.All(root.GetProperty("families").EnumerateArray(), f => Assert.True(f.GetProperty("coverage_caveat").GetBoolean()));
                var cxRow = Assert.Single(root.GetProperty("facts").EnumerateArray(), f => f.GetProperty("key").GetString() == "CXPACKET");
                Assert.Equal("comparison_only", cxRow.GetProperty("presence").GetString());
                Assert.Equal("presence", cxRow.GetProperty("band_source").GetString());
                Assert.Equal("worse", cxRow.GetProperty("status").GetString());
                Assert.Equal("parallelism", cxRow.GetProperty("family").GetString());
                /* BLOCKING_EVENTS at 10/hr (base 0.5) is the other comparison-only key that registers; the
                   two are two families (parallelism, lock_contention), so families_worse counts causes. */
                var blockingRow = Assert.Single(root.GetProperty("facts").EnumerateArray(), f => f.GetProperty("key").GetString() == "BLOCKING_EVENTS");
                Assert.Equal("worse", blockingRow.GetProperty("status").GetString());
                Assert.Equal("lock_contention", blockingRow.GetProperty("family").GetString());
                Assert.True(root.GetProperty("summary").GetProperty("new_issues").GetInt32() >= 2);
                Assert.True(root.GetProperty("summary").GetProperty("families_worse").GetInt32() >= 2);
                Assert.Contains("N=1 vs N=1", root.GetProperty("reading").GetString()!, StringComparison.Ordinal);
                Assert.Contains("robust-sigma", root.GetProperty("band_rules").GetProperty("baseline").GetString()!, StringComparison.Ordinal);
            }

            /* ── zero coverage: a 2h window anchored inside the dead stretch composes with the #3524
               envelope — unavailable, never an all-clear. */
            var anchor = DateTime.SpecifyKind(windowEnd, DateTimeKind.Utc).ToString("o");
            var dead = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, 2, anchor);
            using (var doc = JsonDocument.Parse(dead))
            {
                Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
            }
            Assert.Contains("NOT an all-clear", dead, StringComparison.Ordinal);
            Assert.DoesNotContain("within normal ranges", dead, StringComparison.Ordinal);

            /* ── the control: fill the remaining three hours and the same window is fully covered, the
               storm reads the same 0.25 (it was the same storm), blocking drops to its true 2.5/hr over
               four observed hours, and the gap fact is gone. */
            for (var i = 5; i <= 16; i++)
                await PlantWaitAsync(connection, windowStart.AddMinutes(15 * i), "CXPACKET", 225_000L, ct);

            var fullContext = new AnalysisContext
            {
                ServerId = ServerId,
                ServerName = ServerName,
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero
            };
            var fullFacts = await collector.CollectFactsAsync(fullContext);

            Assert.Equal(1.0, fullContext.Coverage!.Fraction, precision: 6);
            Assert.False(fullContext.Coverage.IsPartial);
            Assert.DoesNotContain(fullFacts, f => f.Key == WindowCoverage.FactKey);
            Assert.Equal(0.25, Assert.Single(fullFacts, f => f.Key == "CXPACKET").Value, precision: 6);
            Assert.Equal(2.5, Assert.Single(fullFacts, f => f.Key == "BLOCKING_EVENTS").Value, precision: 6);

            var fullAnalysis = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, 4);
            using (var doc = JsonDocument.Parse(fullAnalysis))
            {
                Assert.Equal(JsonValueKind.Null, doc.RootElement.GetProperty("caveat").ValueKind);
                Assert.False(doc.RootElement.GetProperty("coverage").GetProperty("partial").GetBoolean());
            }

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

    private static async Task RegisterServerAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, 16, $4, $4)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE, sql_major_version = 16;", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantWaitAsync(
        NpgsqlConnection connection, DateTime at, string waitType, long deltaWaitMs, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, 0)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(waitType);
        command.Parameters.AddWithValue(deltaWaitMs > 0 ? 10L : 0L);
        command.Parameters.AddWithValue(deltaWaitMs);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantBlockingAsync(NpgsqlConnection connection, DateTime at, int blockedSpid, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time,
     wait_time_ms, blocking_spid, blocked_spid, blocking_status, database_name)
VALUES ($1, $2, $3, $4, $2, 12000, 60, $5, 'suspended', 'AppDb')", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(blockedSpid);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM wait_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM blocked_process_reports WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
