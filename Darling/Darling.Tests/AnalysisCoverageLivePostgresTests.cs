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

            /* #3871's ≤4h degeneracy, pinned on the pre-existing fixture: a 4-hour window is ONE bucket
               under the window-start origin, so the peak IS the window and the graded value is exactly
               what this test always asserted. The peak keys ride beside the whole-window context. */
            Assert.Equal(10.0, blocking.Metadata["events_per_hour_peak_4h"], precision: 6);
            Assert.Equal(10.0, blocking.Metadata["peak_4h_event_count"], precision: 6);
            Assert.Equal(10.0, blocking.Metadata["events_per_hour"], precision: 6);

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
               window partly. #7458: nothing is compared. The baseline caveat leads, says the window was
               not collected at all and names it, the partly collected comparison still earns its own
               sentence, and the gap fact is reported through the coverage blocks rather than as a key. */
            var compared = await DarlingMcpTools.CompareAnalysis(service, postgres, ServerName, 4, 28);
            using (var doc = JsonDocument.Parse(compared))
            {
                var root = doc.RootElement;
                var caveat = root.GetProperty("caveat").GetString()!;
                Assert.StartsWith("The BASELINE window (", caveat, StringComparison.Ordinal);
                Assert.Contains("was not collected at all", caveat, StringComparison.Ordinal);
                Assert.DoesNotContain("The BASELINE window was only partly collected", caveat, StringComparison.Ordinal);
                Assert.DoesNotContain("counts as a new issue", caveat, StringComparison.Ordinal);
                Assert.Contains("The COMPARISON window was only partly collected", caveat, StringComparison.Ordinal);
                Assert.True(root.GetProperty("baseline").GetProperty("coverage").GetProperty("unobserved").GetBoolean());
                Assert.True(root.GetProperty("comparison").GetProperty("coverage").GetProperty("partial").GetBoolean());
                Assert.DoesNotContain(root.GetProperty("facts").EnumerateArray(),
                    f => f.GetProperty("key").GetString() == WindowCoverage.FactKey);

                AssertNothingCompared(root);
                Assert.All(root.GetProperty("facts").EnumerateArray(), f => Assert.True(f.GetProperty("coverage_caveat").GetBoolean()));
                var cxRow = Assert.Single(root.GetProperty("facts").EnumerateArray(), f => f.GetProperty("key").GetString() == "CXPACKET");
                Assert.Equal("comparison_only", cxRow.GetProperty("presence").GetString());
                Assert.Equal("not_compared", cxRow.GetProperty("status").GetString());
                /* The tool's window ends at "now", a minute past windowEnd, so the value is a hair over 0.25
                   (0.2549 measured); a precision-1 compare rounds it to 0.3 against 0.2. */
                Assert.InRange(cxRow.GetProperty("comparison_value").GetDouble(), 0.24, 0.27);
                Assert.Equal("parallelism", cxRow.GetProperty("family").GetString());
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

    /// <summary>
    /// #3871: the same storm must grade the same on every pass length. 232 blocked-process reports inside
    /// ONE hour of an otherwise-quiet, fully-observed 24-hour window: the whole-window average reads
    /// 232/24 ≈ 9.7/hr — under the concern bar, the dilution the issue was filed about — while the graded
    /// value is the busiest 4-hour sub-window's rate, 232/4 = 58/hr, past the CRITICAL bar (50) the pair
    /// pins at its measured 4-hour grain. The steady day is the control: 3/hr across all 24 hours puts 12
    /// events in every bucket, 12/4 = 3/hr, under the concern bar (10) exactly as the average always said.
    /// </summary>
    [Fact]
    public async Task AOneHourStorm_GradesTheSame_OnAFourHourAndATwentyFourHourPass()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live storm-grain test.");

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

            var stormEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var stormStart = stormEnd.AddHours(-24);

            /* Lifetime-history gate, then full coverage: a wait sample every 15 minutes across the whole
               day, so observed hours ≈ 24 and the peak divisor is min(4, 24) = 4 — the constant's own
               grain, not a gap artifact. */
            await PlantWaitAsync(connection, stormEnd.AddHours(-40), "OLD_WAIT", 1_000L, ct);
            for (var i = 0; i <= 96; i++)
                await PlantWaitAsync(connection, stormStart.AddMinutes(15 * i), "CXPACKET", i == 0 ? 0L : 1_000L, ct);

            /* The storm: 232 reports inside hour six — wholly inside the [4h, 8h) bucket. */
            for (var i = 0; i < 232; i++)
                await PlantBlockingAsync(connection, stormStart.AddHours(6).AddSeconds(i * 15), 100 + (i % 50), ct);

            var stormContext = new AnalysisContext
            {
                ServerId = ServerId,
                ServerName = ServerName,
                TimeRangeStart = stormStart,
                TimeRangeEnd = stormEnd,
                ServerUtcOffset = TimeSpan.Zero
            };
            var stormFacts = await new PgFactCollector(postgres).CollectFactsAsync(stormContext);

            var storm = Assert.Single(stormFacts, f => f.Key == "BLOCKING_EVENTS");
            Assert.Equal(58.0, storm.Value, precision: 6);
            Assert.Equal(58.0, storm.Metadata["events_per_hour_peak_4h"], precision: 6);
            Assert.Equal(232.0, storm.Metadata["peak_4h_event_count"], precision: 6);
            /* The context the average always told — and the grade it used to produce. */
            Assert.InRange(storm.Metadata["events_per_hour"], 9.0, 10.0);
            Assert.True(storm.Value >= 50.0, "the storm must sit past the CRITICAL bar at the pair's own grain");

            /* The control: the day BEFORE, same server, steady 3/hr — every 4-hour bucket holds 12. */
            var steadyEnd = stormStart;
            var steadyStart = steadyEnd.AddHours(-24);
            for (var i = 0; i <= 96; i++)
                await PlantWaitAsync(connection, steadyStart.AddMinutes(15 * i), "CXPACKET", i == 0 ? 0L : 1_000L, ct);
            for (var hour = 0; hour < 24; hour++)
                for (var e = 0; e < 3; e++)
                    await PlantBlockingAsync(connection, steadyStart.AddHours(hour).AddMinutes(7 + e * 17), 200 + e, ct);

            var steadyContext = new AnalysisContext
            {
                ServerId = ServerId,
                ServerName = ServerName,
                TimeRangeStart = steadyStart,
                TimeRangeEnd = steadyEnd,
                ServerUtcOffset = TimeSpan.Zero
            };
            var steadyFacts = await new PgFactCollector(postgres).CollectFactsAsync(steadyContext);

            var steady = Assert.Single(steadyFacts, f => f.Key == "BLOCKING_EVENTS");
            Assert.Equal(3.0, steady.Value, precision: 6);
            Assert.Equal(12.0, steady.Metadata["peak_4h_event_count"], precision: 6);
            Assert.True(steady.Value < 10.0, "a steady 3/hr day must stay under the concern bar");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #7895 on the real store: the BLOCKING_EVENTS peak divides each 4 h bucket's waits by that bucket's own covered
    /// hours, its span clipped to the window end and never under one hour, and the peak is the busiest rate, not the
    /// biggest count. A 6 h read is a full bucket and a two-hour tail: thirty waits in the tail are 15/h, and they
    /// outrank forty waits spread over the full bucket (10/h). Lite's <c>BlockingPeakBucketHoursTests</c> is the twin.
    /// </summary>
    [Fact]
    public async Task ASixHourWindow_PeaksOnTheTailBucketsOwnHours_AndOnRateNotCount()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live peak-bucket test.");

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

            var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var windowStart = windowEnd.AddHours(-6);

            /* Full coverage: a wait sample every fifteen minutes across the six hours. */
            for (var i = 0; i <= 24; i++)
                await PlantWaitAsync(connection, windowStart.AddMinutes(15 * i), "CXPACKET", i == 0 ? 0L : 1_000L, ct);

            /* Forty waits across the full [0h, 4h) bucket and thirty inside the two-hour [4h, 6h) tail. */
            for (var i = 0; i < 40; i++)
                await PlantBlockingAsync(connection, windowStart.AddMinutes(6 * i), 100 + i, ct);
            for (var i = 0; i < 30; i++)
                await PlantBlockingAsync(connection, windowStart.AddHours(4).AddMinutes(3 * i), 200 + i, ct);

            var context = new AnalysisContext
            {
                ServerId = ServerId,
                ServerName = ServerName,
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero
            };
            var facts = await new PgFactCollector(postgres).CollectFactsAsync(context);

            var blocking = Assert.Single(facts, f => f.Key == "BLOCKING_EVENTS");
            Assert.Equal(70.0, blocking.Metadata["event_count"], precision: 6);
            Assert.Equal(15.0, blocking.Value, precision: 6);
            Assert.Equal(15.0, blocking.Metadata["events_per_hour_peak_4h"], precision: 6);
            Assert.Equal(30.0, blocking.Metadata["peak_4h_event_count"], precision: 6);
            Assert.True(blocking.Metadata.TryGetValue("peak_4h_covered_hours", out var covered), "the fact carries the peak bucket's covered hours");
            Assert.Equal(2.0, covered, precision: 6);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #7895 on the real store: a tail bucket shorter than an hour is judged as an hour, so three waits in a
    /// thirty-minute tail are 3/h, not 6/h.
    /// </summary>
    [Fact]
    public async Task ATailBucketUnderAnHour_IsJudgedAsOneHour()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live peak-bucket floor test.");

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

            var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var windowStart = windowEnd.AddMinutes(-270);

            for (var i = 0; i <= 18; i++)
                await PlantWaitAsync(connection, windowStart.AddMinutes(15 * i), "CXPACKET", i == 0 ? 0L : 1_000L, ct);
            for (var i = 0; i < 3; i++)
                await PlantBlockingAsync(connection, windowStart.AddHours(4).AddMinutes(5 * i), 100 + i, ct);

            var context = new AnalysisContext
            {
                ServerId = ServerId,
                ServerName = ServerName,
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero
            };
            var facts = await new PgFactCollector(postgres).CollectFactsAsync(context);

            var blocking = Assert.Single(facts, f => f.Key == "BLOCKING_EVENTS");
            Assert.Equal(3.0, blocking.Value, precision: 6);
            Assert.Equal(3.0, blocking.Metadata["peak_4h_event_count"], precision: 6);
            Assert.True(blocking.Metadata.TryGetValue("peak_4h_covered_hours", out var covered), "the fact carries the peak bucket's covered hours");
            Assert.Equal(1.0, covered, precision: 6);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #7895 on the real store: a full bucket the collector saw end to end divides by its own four hours whatever the
    /// rest of the window did. The collector was up for the first six hours of a 24 h window only, and forty waits
    /// sit in the fully observed first bucket: 10/h, the rate a fully observed day reads for the same bucket, not
    /// 40/h from a divisor shrunk by the window's one coverage fraction. Lite's <c>BlockingPeakBucketHoursTests</c>
    /// is the twin.
    /// </summary>
    [Fact]
    public async Task AQuarterObservedDay_ReadsAFullBucketAtItsOwnFourHours()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live quarter-observed-day test.");

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

            var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var windowStart = windowEnd.AddHours(-24);

            /* A wait sample every fifteen minutes for the first six hours only: observed 6 h of 24, coverage 0.25. */
            for (var i = 0; i <= 24; i++)
                await PlantWaitAsync(connection, windowStart.AddMinutes(15 * i), "CXPACKET", i == 0 ? 0L : 1_000L, ct);

            /* Forty waits across the fully observed [0h, 4h) bucket. */
            for (var i = 0; i < 40; i++)
                await PlantBlockingAsync(connection, windowStart.AddMinutes(6 * i), 100 + i, ct);

            var context = new AnalysisContext
            {
                ServerId = ServerId,
                ServerName = ServerName,
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero
            };
            var facts = await new PgFactCollector(postgres).CollectFactsAsync(context);

            var blocking = Assert.Single(facts, f => f.Key == "BLOCKING_EVENTS");
            Assert.Equal(0.25, blocking.Metadata["observed_hours"] / blocking.Metadata["period_hours"], precision: 6);
            Assert.Equal(10.0, blocking.Value, precision: 6);
            Assert.Equal(10.0, blocking.Metadata["events_per_hour_peak_4h"], precision: 6);
            Assert.Equal(40.0, blocking.Metadata["peak_4h_event_count"], precision: 6);
            Assert.True(blocking.Metadata.TryGetValue("peak_4h_covered_hours", out var covered), "the fact carries the peak bucket's covered hours");
            Assert.Equal(4.0, covered, precision: 6);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #7178 on the real store: the engine's LCK_M_SCH_M wait reaches its own SCH_M fact instead of
    /// folding into LCK with the writer/writer lock modes, so the SCH_M scorer ramp, advice card and
    /// RUNNING_JOBS edge can fire. LCK_M_SCH_S still folds into LCK. Lite's
    /// <c>FactCollectorMiseryTests</c> is the twin on DuckDB.
    /// </summary>
    [Fact]
    public async Task SchemaModificationWait_GetsItsOwnSchMFact_NotLck()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live schema-lock test.");

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

            var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var windowStart = windowEnd.AddHours(-1);

            await PlantWaitAsync(connection, windowEnd.AddHours(-30), "OLD_WAIT", 1_000L, ct);

            /* A baseline reading, then one delta per wait type across the fully observed hour. */
            foreach (var type in new[] { "LCK_M_SCH_M", "LCK_M_SCH_S", "LCK_M_X" })
                await PlantWaitAsync(connection, windowStart, type, 0L, ct);
            for (var i = 1; i <= 4; i++)
            {
                await PlantWaitAsync(connection, windowStart.AddMinutes(15 * i), "LCK_M_SCH_M", 100_000L, ct);
                await PlantWaitAsync(connection, windowStart.AddMinutes(15 * i), "LCK_M_SCH_S", 50_000L, ct);
                await PlantWaitAsync(connection, windowStart.AddMinutes(15 * i), "LCK_M_X", 25_000L, ct);
            }

            var context = new AnalysisContext
            {
                ServerId = ServerId,
                ServerName = ServerName,
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero
            };
            var facts = await new PgFactCollector(postgres).CollectFactsAsync(context);

            var schM = Assert.Single(facts, f => f.Source == "waits" && f.Key == "SCH_M");
            Assert.Equal(400_000, schM.Metadata["wait_time_ms"]);
            Assert.DoesNotContain(facts, f => f.Key == "LCK_M_SCH_M");

            var lck = Assert.Single(facts, f => f.Source == "waits" && f.Key == "LCK");
            Assert.Equal(300_000, lck.Metadata["wait_time_ms"]); // SCH_S + X, not SCH_M
            Assert.False(lck.Metadata.ContainsKey("LCK_M_SCH_M_ms"));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #7458 on the real read: a baseline fully collected and a comparison window collected for one hour of four.
    /// A partly collected side still compares (the storm is worse, the reply is comparable). Move the baseline
    /// back to a stretch nobody collected and nothing compares. Move the comparison to one and the baseline's
    /// facts do not become resolved issues.
    /// </summary>
    [Fact]
    public async Task CompareAnalysis_APartlyCollectedSideStillCompares_AndAnUnobservedSideDoesNot()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live compare test.");

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

            var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            await PlantWaitAsync(connection, windowEnd.AddHours(-60), "OLD_WAIT", 1_000L, ct);

            /* Baseline [end-28h, end-24h]: fully collected, 900,000 ms of CXPACKET over 4 observed hours. */
            var baselineStart = windowEnd.AddHours(-28);
            await PlantWaitAsync(connection, baselineStart, "CXPACKET", 0L, ct);
            for (var i = 1; i <= 16; i++)
                await PlantWaitAsync(connection, baselineStart.AddMinutes(15 * i), "CXPACKET", 56_250L, ct);

            /* Comparison [end-4h, end]: one hour collected, the same 900,000 ms in it. */
            var comparisonStart = windowEnd.AddHours(-4);
            await PlantWaitAsync(connection, comparisonStart, "CXPACKET", 0L, ct);
            for (var i = 1; i <= 4; i++)
                await PlantWaitAsync(connection, comparisonStart.AddMinutes(15 * i), "CXPACKET", 225_000L, ct);

            var service = new DarlingAnalysisService(postgres);
            var asOf = DateTime.SpecifyKind(windowEnd, DateTimeKind.Utc).ToString("o");

            /* A partly collected comparison against a full baseline: still compared. */
            var partial = await DarlingMcpTools.CompareAnalysis(service, postgres, ServerName, 4, 28, asOf, ct);
            using (var doc = JsonDocument.Parse(partial))
            {
                var root = doc.RootElement;
                Assert.True(root.GetProperty("comparable").GetBoolean());
                Assert.True(root.GetProperty("comparison").GetProperty("coverage").GetProperty("partial").GetBoolean());
                Assert.Contains("The COMPARISON window was only partly collected", root.GetProperty("caveat").GetString()!, StringComparison.Ordinal);
                Assert.Equal(0, root.GetProperty("summary").GetProperty("not_compared").GetInt32());
                var cx = Assert.Single(root.GetProperty("facts").EnumerateArray(), f => f.GetProperty("key").GetString() == "CXPACKET");
                Assert.Equal("both", cx.GetProperty("presence").GetString());
                Assert.Equal("worse", cx.GetProperty("status").GetString());
            }

            /* A baseline stretch nobody collected (52h back): nothing compares. */
            var noBaseline = await DarlingMcpTools.CompareAnalysis(service, postgres, ServerName, 4, 56, asOf, ct);
            using (var doc = JsonDocument.Parse(noBaseline))
            {
                var root = doc.RootElement;
                Assert.False(root.GetProperty("comparable").GetBoolean());
                Assert.True(root.GetProperty("baseline").GetProperty("coverage").GetProperty("unobserved").GetBoolean());
                var caveat = root.GetProperty("caveat").GetString()!;
                Assert.StartsWith("The BASELINE window (", caveat, StringComparison.Ordinal);
                Assert.Contains("was not collected at all", caveat, StringComparison.Ordinal);
                Assert.DoesNotContain("The BASELINE window was only partly collected", caveat, StringComparison.Ordinal);
                AssertNothingCompared(root);
            }

            /* A comparison stretch nobody collected (end-18h, baseline end-28h..end-24h): the baseline's CXPACKET
               is not a resolved issue. */
            var noComparison = await DarlingMcpTools.CompareAnalysis(
                service, postgres, ServerName, 4, 10,
                DateTime.SpecifyKind(windowEnd.AddHours(-18), DateTimeKind.Utc).ToString("o"), ct);
            using (var doc = JsonDocument.Parse(noComparison))
            {
                var root = doc.RootElement;
                Assert.False(root.GetProperty("comparable").GetBoolean());
                Assert.True(root.GetProperty("comparison").GetProperty("coverage").GetProperty("unobserved").GetBoolean());
                var caveat = root.GetProperty("caveat").GetString()!;
                Assert.StartsWith("The COMPARISON window (", caveat, StringComparison.Ordinal);
                Assert.Contains("was not collected at all", caveat, StringComparison.Ordinal);
                AssertNothingCompared(root);
                var cx = Assert.Single(root.GetProperty("facts").EnumerateArray(), f => f.GetProperty("key").GetString() == "CXPACKET");
                Assert.Equal("baseline_only", cx.GetProperty("presence").GetString());
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #7893 on the real store: the DEADLOCKS value is the busiest one-hour bucket of the window, the server-hour grain
    /// the (5, 20) pair was measured on, so twenty deadlocks inside one hour grade the same on a 4 hour pass and a 24 hour
    /// read. The whole-window average stays published as <c>deadlocks_per_hour</c>. Lite's <c>DeadlockPeakHourTests</c> is
    /// the twin.
    /// </summary>
    [Fact]
    public async Task AStormOfTwentyDeadlocksInOneHour_GradesTheSame_OnAFourHourAndATwentyFourHourWindow()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live deadlock peak test.");

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

            var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var dayStart = windowEnd.AddHours(-24);

            /* Full coverage: a wait sample every fifteen minutes across the day. */
            for (var i = 0; i <= 96; i++)
                await PlantWaitAsync(connection, dayStart.AddMinutes(15 * i), "CXPACKET", i == 0 ? 0L : 1_000L, ct);

            /* Twenty deadlocks inside the hour [End-2h, End-1h): one bucket under the window-start origin on both windows. */
            for (var i = 0; i < 20; i++)
                await PlantDeadlockAsync(connection, windowEnd.AddHours(-2).AddMinutes(3 * i), ct);

            var collector = new PgFactCollector(postgres);
            var fourHours = Assert.Single(await collector.CollectFactsAsync(Context(windowEnd.AddHours(-4), windowEnd)), f => f.Key == "DEADLOCKS");
            var day = Assert.Single(await collector.CollectFactsAsync(Context(dayStart, windowEnd)), f => f.Key == "DEADLOCKS");

            Assert.Equal(20.0, fourHours.Value, precision: 6);
            Assert.Equal(20.0, day.Value, precision: 6);

            Assert.Equal(20.0, day.Metadata["deadlock_count"], precision: 6);
            Assert.Equal(5.0, fourHours.Metadata["deadlocks_per_hour"], precision: 6);
            Assert.Equal(20.0 / 24.0, day.Metadata["deadlocks_per_hour"], precision: 6);
            Assert.True(day.Metadata.TryGetValue("deadlocks_per_hour_peak_1h", out var peakRate), "the fact carries the peak hour's rate");
            Assert.Equal(20.0, peakRate, precision: 6);
            Assert.True(day.Metadata.TryGetValue("peak_1h_deadlock_count", out var peakCount), "the fact carries the peak hour's count");
            Assert.Equal(20.0, peakCount, precision: 6);

            /* The Azure SQL Database master path, which skips deadlocks wholly inside the separately monitored databases,
               grades the same peak: these deadlocks belong to another database. */
            var scoped = Context(dayStart, windowEnd);
            scoped.SeparatelyMonitoredDatabases = ["SeparateDb"];
            var scopedFact = Assert.Single(await collector.CollectFactsAsync(scoped), f => f.Key == "DEADLOCKS");
            Assert.Equal(20.0, scopedFact.Metadata["deadlock_count"], precision: 6);
            Assert.Equal(20.0, scopedFact.Value, precision: 6);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #7893 on the real store with the peak and the count apart: twelve deadlocks inside one hour and three inside
    /// another. On the scoped path the busy hour mixes rows kept by their database name with rows kept by their graph,
    /// so the two arms are bucketed together, and rows wholly inside the separately monitored database are neither
    /// counted nor bucketed. The unscoped read of the same rows counts every one and finds the same busiest hour.
    /// Lite's <c>DeadlockPeakHourTests</c> is the twin.
    /// </summary>
    [Fact]
    public async Task TwelveDeadlocksInOneHourAndThreeInAnother_PeakIsTwelve_AndTheCountIsFifteen_OnBothPaths()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live deadlock bucketing test.");

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

            var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var dayStart = windowEnd.AddHours(-24);

            /* Full coverage: a wait sample every fifteen minutes across the day. */
            for (var i = 0; i <= 96; i++)
                await PlantWaitAsync(connection, dayStart.AddMinutes(15 * i), "CXPACKET", i == 0 ? 0L : 1_000L, ct);

            /* The busy hour: seven rows the database name alone keeps, and five whose graph has to be read. */
            var busy = dayStart.AddHours(6);
            for (var i = 0; i < 7; i++)
                await PlantDeadlockAsync(connection, busy.AddMinutes(4 * i), "AppDb", "<deadlock/>", ct);
            for (var i = 0; i < 5; i++)
                await PlantDeadlockAsync(connection, busy.AddMinutes(30 + (4 * i)), null, Graph("AppDb", "SeparateDb"), ct);

            /* The quiet hour: three master-stamped rows whose graph keeps them, and four wholly inside the separately
               monitored database, which the scoped path drops. */
            var quiet = dayStart.AddHours(20);
            for (var i = 0; i < 3; i++)
                await PlantDeadlockAsync(connection, quiet.AddMinutes(5 * i), "master", Graph("AppDb"), ct);
            for (var i = 0; i < 4; i++)
                await PlantDeadlockAsync(connection, quiet.AddMinutes(30 + (5 * i)), "SeparateDb", Graph("SeparateDb"), ct);

            var collector = new PgFactCollector(postgres);

            var scoped = Context(dayStart, windowEnd);
            scoped.SeparatelyMonitoredDatabases = ["SeparateDb"];
            var scopedFact = Assert.Single(await collector.CollectFactsAsync(scoped), f => f.Key == "DEADLOCKS");
            Assert.Equal(15.0, scopedFact.Metadata["deadlock_count"], precision: 6);
            Assert.Equal(12.0, scopedFact.Value, precision: 6);
            Assert.Equal(12.0, scopedFact.Metadata["peak_1h_deadlock_count"], precision: 6);

            var all = Assert.Single(await collector.CollectFactsAsync(Context(dayStart, windowEnd)), f => f.Key == "DEADLOCKS");
            Assert.Equal(19.0, all.Metadata["deadlock_count"], precision: 6);
            Assert.Equal(12.0, all.Value, precision: 6);
            Assert.Equal(12.0, all.Metadata["peak_1h_deadlock_count"], precision: 6);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>The deadlock tests' analysis window, with no server offset so the planted times are the window's own.</summary>
    private static AnalysisContext Context(DateTime start, DateTime end) => new()
    {
        ServerId = ServerId,
        ServerName = ServerName,
        TimeRangeStart = start,
        TimeRangeEnd = end,
        ServerUtcOffset = TimeSpan.Zero
    };

    private static string Graph(params string[] databases) =>
        "<deadlock><victim-list/><process-list>"
        + string.Concat(databases.Select((d, i) => $"<process id=\"p{i}\" currentdbname=\"{d}\"/>"))
        + "</process-list></deadlock>";

    private static void AssertNothingCompared(JsonElement root)
    {
        var summary = root.GetProperty("summary");
        Assert.Equal(0, summary.GetProperty("worse").GetInt32());
        Assert.Equal(0, summary.GetProperty("better").GetInt32());
        Assert.Equal(0, summary.GetProperty("new_issues").GetInt32());
        Assert.Equal(0, summary.GetProperty("resolved_issues").GetInt32());
        Assert.Equal(0, summary.GetProperty("families_worse").GetInt32());
        Assert.Equal(0, summary.GetProperty("families_better").GetInt32());
        Assert.True(summary.GetProperty("not_compared").GetInt32() > 0);
        Assert.All(root.GetProperty("facts").EnumerateArray(), f => Assert.Equal("not_compared", f.GetProperty("status").GetString()));
        Assert.All(root.GetProperty("families").EnumerateArray(), f => Assert.Equal("not_compared", f.GetProperty("status").GetString()));
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

    /// <summary>One deadlock of the application database, which is never a separately monitored one here.</summary>
    private static Task PlantDeadlockAsync(NpgsqlConnection connection, DateTime at, CancellationToken ct) =>
        PlantDeadlockAsync(connection, at, "AppDb", "<deadlock/>", ct);

    /// <summary>One deadlock stamped with <paramref name="database"/> (null leaves the column NULL) and carrying
    /// <paramref name="graphXml"/>: the scoped path keeps a row by its name when the name proves it is outside the
    /// separately monitored databases, and otherwise by its graph.</summary>
    private static async Task PlantDeadlockAsync(NpgsqlConnection connection, DateTime at, string? database, string graphXml, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO deadlocks
    (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml, database_name)
VALUES ($1, $2, $3, $4, $2, $5, $6)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(graphXml);
        command.Parameters.AddWithValue((object?)database ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM wait_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM blocked_process_reports WHERE server_id = {ServerId}; " +
            $"DELETE FROM deadlocks WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
