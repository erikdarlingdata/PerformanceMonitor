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
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Gated (DARLING_TEST_PG) proof of #3524: <c>analyze_server</c> must not answer "all metrics are
/// within normal ranges" when the analysis window collected NOTHING. The pipeline's data-span gate
/// measures LIFETIME history, so a server whose collection died (broken credential, unreachable
/// target) still passes it — and the zero-facts pass used to return a bare <c>[]</c> that the tool
/// rendered as a true-negative all-clear. Both sides of the distinction are asserted, on the REAL
/// 24h gate rather than a zeroed one, because the bug lives precisely in the gap between "enough
/// history" and "an empty window".
///
/// <para>#3653 adds the third kind of nothing, which the first fix got wrong once every collector stamped
/// coverage (#3592/#3665): a window the collector WAS up for, over which nothing rose to a fact. That is
/// a measurement, and it must not wear the dead-collector envelope. The unobserved-window-with-point-in-
/// time-facts arm is <c>DarlingAnalysisServiceEngineRoutingTests</c>' stamped-row pass (one in-window row,
/// no interval) and Lite's <c>AnalysisCoverageTests</c>.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class AnalyzeWindowEmptyLivePostgresTests
{
    private const string ServerName = "darling-analyze-window-empty-e2e";
    private const string StaleWait = "WE3524_STALE_WAIT";
    private const string BenignWait = "WE3524_BENIGN_WAIT";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ADeadCollectorWindow_IsUnavailable_AndAWindowWithFactsKeepsTheAllClear()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live window-empty analysis test.");

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
            var service = new DarlingAnalysisService(postgres);

            /* ── the dead-collector shape: 25 hours of history (the real 24h gate PASSES) whose
               newest row is five hours old, so the tool's default 4h window holds nothing. */
            await PlantWaitAsync(connection, Naive(DateTime.UtcNow.AddHours(-30)), StaleWait, 60_000L, ct);
            await PlantWaitAsync(connection, Naive(DateTime.UtcNow.AddHours(-5)), StaleWait, 60_000L, ct);

            var deadWindow = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName);
            using (var doc = JsonDocument.Parse(deadWindow))
            {
                Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());

                /* The #2506 persistence disclosure survives the new envelope — an anchored
                   empty-window run still owes the caller that context, and this unanchored one
                   reports the ordinary answer. */
                Assert.True(doc.RootElement.GetProperty("hints").GetProperty("persisted").GetBoolean());
            }
            Assert.Contains("get_collection_health", deadWindow, StringComparison.Ordinal);
            Assert.Contains("NOT an all-clear", deadWindow, StringComparison.Ordinal);
            Assert.DoesNotContain("within normal ranges", deadWindow, StringComparison.Ordinal);

            /* The service says WHICH kind of nothing this was: the window, not the lifetime span. */
            Assert.NotNull(service.WindowEmptyMessage);
            Assert.Null(service.InsufficientDataMessage);

            /* ── the control, and the reason the fix is a distinction rather than a rewording: the
               SAME server with a benign wait collected THROUGHOUT the window has facts to score, finds
               nothing wrong, and keeps the genuine true-negative all-clear.

               A realistic series — a reading every fifteen minutes from the window's start to now —
               rather than the single row this control originally planted, because since #3538 A2 the
               engine divides by the time the collector actually observed, and a lone reading with
               nothing before it observes no time at all (its delta was the calculator's first
               sighting). A single in-window row is now, correctly, the dead-collector shape; the
               all-clear is earned by a window the collector was up for. */
            var now = DateTime.UtcNow;
            for (var minutesAgo = 240; minutesAgo >= 0; minutesAgo -= 15)
                await PlantWaitAsync(connection, Naive(now.AddMinutes(-minutesAgo)), BenignWait, 100L, ct);

            var healthyWindow = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName);
            using (var doc = JsonDocument.Parse(healthyWindow))
            {
                Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());

                /* #3538 A2: the all-clear now says how much of the window it speaks for. */
                Assert.InRange(
                    doc.RootElement.GetProperty("hints").GetProperty("coverage").GetProperty("observed_fraction").GetDouble(),
                    0.99, 1.0);
            }
            Assert.Contains("All metrics are within normal ranges", healthyWindow, StringComparison.Ordinal);
            Assert.DoesNotContain("PARTIAL", healthyWindow, StringComparison.Ordinal);
            Assert.Null(service.WindowEmptyMessage);
            Assert.NotNull(service.LastWindowCoverage);
            Assert.False(service.LastWindowCoverage!.IsPartial);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #3653: the collector was UP for the whole window — a reading every fifteen minutes, so the coverage
    /// witness finds intervals inside it — but every reading carries ZERO accrued wait time, so no wait
    /// fact is emitted (<c>delta_wait_time_ms &gt; 0</c> is the wait read's filter) and no other family has
    /// a row for this server to read (the metadata fact reads <c>server_properties</c>, not the registry):
    /// an observed window that produced no fact at all. Until #3653 the service's rule was
    /// <c>facts.Count == 0 || ObservedDurationMs &lt;= 0</c>, and the first half made this window wear the
    /// dead-collector envelope — "collection appears to have stopped or broken … NOT an all-clear",
    /// rendered <c>unavailable</c> with a pointer at collection health, and written by the worker as the
    /// marker the Viewer renders — for a collector the witness proves was running. It is <c>empty</c>: an
    /// all-clear that rests on the coverage the payload states, with none of the dead-collector prose, and
    /// the service's one log line for it is Information, names the coverage, and says what the all-clear
    /// rests on.
    ///
    /// <para>The fact count is asserted directly through <c>CollectAndScoreFactsAsync</c> so this arm is
    /// known to exercise the zero-facts path and not merely "facts but no findings", which the control
    /// above already covers.</para>
    /// </summary>
    [Fact]
    public async Task AnObservedWindowWithNoFacts_IsAnAllClearAtItsCoverage_NotADeadCollector()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live window-empty analysis test.");

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
            var logger = new CapturingTestLogger();
            var service = new DarlingAnalysisService(postgres, logger: logger);

            /* 30 hours of history (the real 24h gate PASSES); the window itself is fully observed — a
               reading every fifteen minutes — and every reading accrued nothing. */
            await PlantWaitAsync(connection, Naive(DateTime.UtcNow.AddHours(-30)), StaleWait, 60_000L, ct);
            var now = DateTime.UtcNow;
            for (var minutesAgo = 240; minutesAgo >= 0; minutesAgo -= 15)
                await PlantWaitAsync(connection, Naive(now.AddMinutes(-minutesAgo)), BenignWait, 0L, ct);

            /* The precondition this arm exists for: observed time, zero facts. */
            var (facts, coverage, _) = await service.CollectAndScoreFactsAsync(ServerId, ServerName);
            Assert.Empty(facts);
            Assert.NotNull(coverage);
            Assert.True(coverage!.IsObserved);
            Assert.False(coverage.IsPartial);

            var observedWindow = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName);
            using (var doc = JsonDocument.Parse(observedWindow))
            {
                Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());

                /* The all-clear says which coverage it rests on. */
                var payloadCoverage = doc.RootElement.GetProperty("hints").GetProperty("coverage");
                Assert.False(payloadCoverage.GetProperty("unobserved").GetBoolean());
                Assert.False(payloadCoverage.GetProperty("partial").GetBoolean());
                Assert.InRange(payloadCoverage.GetProperty("observed_fraction").GetDouble(), 0.99, 1.0);
            }
            Assert.DoesNotContain("NOT an all-clear", observedWindow, StringComparison.Ordinal);
            Assert.DoesNotContain("appears to have stopped", observedWindow, StringComparison.Ordinal);
            Assert.DoesNotContain("get_collection_health", observedWindow, StringComparison.Ordinal);
            Assert.DoesNotContain("PARTIAL", observedWindow, StringComparison.Ordinal);

            /* Neither miss message is set: this pass ran, and the worker's analysis_state marker clears. */
            Assert.Null(service.WindowEmptyMessage);
            Assert.Null(service.InsufficientDataMessage);
            Assert.NotNull(service.LastWindowCoverage);
            Assert.True(service.LastWindowCoverage!.IsObserved);

            /* The scheduled pass has no payload, so the log line is its envelope: Information, not the
               old Warning; names the coverage the witness measured; never says collection may be down. */
            var lines = logger.Joined;
            Assert.Contains("Information: [DarlingAnalysisService] The collector observed the analysis window for " + ServerName, lines, StringComparison.Ordinal);
            Assert.Contains("but emitted no fact over it: the collector observed " + PercentText.Of(service.LastWindowCoverage!.Fraction) + " of this 4h window", lines, StringComparison.Ordinal);
            Assert.Contains("an all-clear from this pass rests on that coverage", lines, StringComparison.Ordinal);
            Assert.DoesNotContain("collection may be down", lines, StringComparison.Ordinal);
            Assert.DoesNotContain("No observed collection in the analysis window", lines, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #8910: a window older than the retention the store keeps is answered as removed by retention, not as a collector that
    /// "may have stopped or broken". The store holds a row at the retention horizon and a recent one, so the span passes the
    /// history gate; <c>as_of</c> puts the window ten days past the horizon. Both tools say so, and neither points at
    /// collection health. A window INSIDE the span with no rows and over a day of history before its end, and a window before
    /// a young store's first row, keep the collection wording: collection may well have missed the first, and retention never
    /// held the second.
    /// </summary>
    [Fact]
    public async Task AWindowOlderThanRetention_SaysRetentionRemovedIt_AndOtherEmptyWindowsKeepTheCollectionText()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live window-empty analysis test.");

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
            var logger = new CapturingTestLogger();
            var service = new DarlingAnalysisService(postgres, logger: logger);

            int retentionDays;
            using (var days = new NpgsqlCommand("SELECT " + DarlingRetentionHorizons.FleetRetentionDaysSql("wait_stats"), connection))
                retentionDays = Convert.ToInt32(await days.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);

            var now = DateTime.UtcNow;
            string AsOf(double daysAgo) => now.AddDays(-daysAgo).ToString("o", System.Globalization.CultureInfo.InvariantCulture);

            /* ── the store as the issue found it: its oldest row sits at the retention horizon (a day past it, as a
               chunk purge leaves it), and a recent row keeps the whole span far over the 24h gate. */
            await PlantWaitAsync(connection, Naive(now.AddDays(-(retentionDays + 1))), StaleWait, 60_000L, ct);
            /* A second row two days later: the history gate measures to a window's end, so the inside window below has a
               day of history before it. */
            await PlantWaitAsync(connection, Naive(now.AddDays(-(retentionDays - 1))), StaleWait, 60_000L, ct);
            await PlantWaitAsync(connection, Naive(now.AddHours(-5)), StaleWait, 60_000L, ct);

            var removed = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, hours_back: 48, as_of: AsOf(retentionDays + 10));
            using (var doc = JsonDocument.Parse(removed))
            {
                Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
                var message = doc.RootElement.GetProperty("message").GetString()!;
                Assert.Contains("This window ends before the oldest row this store keeps (", message, StringComparison.Ordinal);
                Assert.Contains($"{retentionDays} days of retention for wait_stats", message, StringComparison.Ordinal);
                Assert.Contains("Retention removed it, and collection did not break.", message, StringComparison.Ordinal);
                Assert.Contains("NOT an all-clear", message, StringComparison.Ordinal);
                Assert.DoesNotContain("stopped or broken", message, StringComparison.Ordinal);
                Assert.DoesNotContain("get_collection_health", message, StringComparison.Ordinal);
                Assert.DoesNotContain("total collected history", message, StringComparison.Ordinal);
            }
            Assert.True(service.WindowEmptyRemovedByRetention);

            var removedFacts = await DarlingMcpTools.GetAnalysisFacts(service, postgres, ServerName, hours_back: 48, as_of: AsOf(retentionDays + 10));
            using (var doc = JsonDocument.Parse(removedFacts))
            {
                Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
                var message = doc.RootElement.GetProperty("message").GetString()!;
                Assert.Contains("This window ends before the oldest row this store keeps (", message, StringComparison.Ordinal);
                Assert.Contains("Retention removed it, and collection did not break.", message, StringComparison.Ordinal);
                Assert.DoesNotContain("get_collection_health", message, StringComparison.Ordinal);
                Assert.DoesNotContain("may not have run yet", message, StringComparison.Ordinal);
            }

            /* #9152: the service log line reaches the conclusion the answers above did. A window retention removed is an
               Information line that says so, and no Warning says collection may be down. */
            Assert.Contains("Information: [DarlingAnalysisService] No collection in the analysis window for " + ServerName, logger.Joined, StringComparison.Ordinal);
            Assert.Contains("because retention removed it, not because collection is down", logger.Joined, StringComparison.Ordinal);
            Assert.Equal(0, logger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));

            /* ── a window INSIDE the retained span that holds no rows: today's text, today's pointer. */
            var inside = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, hours_back: 4, as_of: AsOf(retentionDays / 2.0));
            using (var doc = JsonDocument.Parse(inside))
            {
                Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
                var message = doc.RootElement.GetProperty("message").GetString()!;
                Assert.Contains("Collection may have stopped or broken", message, StringComparison.Ordinal);
                Assert.Contains("get_collection_health", message, StringComparison.Ordinal);
                Assert.DoesNotContain("Retention removed it", message, StringComparison.Ordinal);
            }
            Assert.False(service.WindowEmptyRemovedByRetention);

            var insideFacts = await DarlingMcpTools.GetAnalysisFacts(service, postgres, ServerName, hours_back: 4, as_of: AsOf(retentionDays / 2.0));
            Assert.DoesNotContain("Retention removed it", insideFacts, StringComparison.Ordinal);

            /* ── a young store: its first row is five days old, far past the horizon, so a window ten days back was
               never held. Retention did not remove it, and the message must not say it did. */
            await DeleteWaitRowsAsync(connection, ct);
            await PlantWaitAsync(connection, Naive(now.AddDays(-5)), StaleWait, 60_000L, ct);
            await PlantWaitAsync(connection, Naive(now.AddHours(-5)), StaleWait, 60_000L, ct);

            var young = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, hours_back: 48, as_of: AsOf(10));
            Assert.DoesNotContain("Retention removed it", young, StringComparison.Ordinal);
            Assert.Contains("Collection may have stopped or broken", young, StringComparison.Ordinal);
            Assert.False(service.WindowEmptyRemovedByRetention);

            /* #9152: the windows retention did not remove keep the Warning. */
            Assert.Contains("Warning: [DarlingAnalysisService] No observed collection in the analysis window for " + ServerName, logger.Joined, StringComparison.Ordinal);
            Assert.Contains("collection may be down", logger.Joined, StringComparison.Ordinal);

            /* ── a store whose purge has stalled: its oldest row sits ten days BEFORE the horizon, so a window before that row
               was never held by a store that has been removing rows. Retention did not remove it either. */
            await DeleteWaitRowsAsync(connection, ct);
            await PlantWaitAsync(connection, Naive(now.AddDays(-(retentionDays + 10))), StaleWait, 60_000L, ct);
            await PlantWaitAsync(connection, Naive(now.AddHours(-5)), StaleWait, 60_000L, ct);

            var stalled = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, hours_back: 48, as_of: AsOf(retentionDays + 20));
            Assert.DoesNotContain("Retention removed it", stalled, StringComparison.Ordinal);
            Assert.Contains("Collection may have stopped or broken", stalled, StringComparison.Ordinal);
            Assert.False(service.WindowEmptyRemovedByRetention);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #9104: <c>get_analysis_facts</c> over a window retention removed answers the same thing in its message and in
    /// <c>hints.truncation_note</c>: retention removed it and collection did not break, with no pointer at collection health. A window
    /// retention did not remove (inside the retained span, before a young store's first row, before a stalled purge's oldest row)
    /// keeps the collection wording in the hint.
    /// </summary>
    [Fact]
    public async Task GetAnalysisFacts_AWindowOlderThanRetention_HintAgreesWithTheMessage_AndOtherEmptyWindowsKeepTheCollectionHint()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live window-empty analysis test.");

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
            var service = new DarlingAnalysisService(postgres);

            int retentionDays;
            using (var days = new NpgsqlCommand("SELECT " + DarlingRetentionHorizons.FleetRetentionDaysSql("wait_stats"), connection))
                retentionDays = Convert.ToInt32(await days.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);

            var now = DateTime.UtcNow;
            string AsOf(double daysAgo) => now.AddDays(-daysAgo).ToString("o", System.Globalization.CultureInfo.InvariantCulture);

            async Task<(string Message, string Note)> FactsAsync(double daysAgo, int hoursBack)
            {
                var json = await DarlingMcpTools.GetAnalysisFacts(service, postgres, ServerName, hours_back: hoursBack, as_of: AsOf(daysAgo));
                using var doc = JsonDocument.Parse(json);
                Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
                return (
                    doc.RootElement.GetProperty("message").GetString()!,
                    doc.RootElement.GetProperty("hints").GetProperty("truncation_note").GetString()!);
            }

            /* The store as retention leaves it: its oldest row at the horizon, and a recent one that keeps the span over the gate. */
            await PlantWaitAsync(connection, Naive(now.AddDays(-(retentionDays + 1))), StaleWait, 60_000L, ct);
            await PlantWaitAsync(connection, Naive(now.AddHours(-5)), StaleWait, 60_000L, ct);

            var removed = await FactsAsync(retentionDays + 10, 48);
            Assert.Contains("Retention removed it, and collection did not break.", removed.Message, StringComparison.Ordinal);
            Assert.Contains("Retention removed it, and collection did not break.", removed.Note, StringComparison.Ordinal);
            Assert.Contains("wait_stats", removed.Note, StringComparison.Ordinal);
            Assert.DoesNotContain("collection may have stopped", removed.Note, StringComparison.Ordinal);
            Assert.DoesNotContain("get_collection_health", removed.Note, StringComparison.Ordinal);

            /* A window inside the retained span that holds no rows: collection may well have missed it. */
            var inside = await FactsAsync(retentionDays / 2.0, 4);
            Assert.DoesNotContain("Retention removed it", inside.Message, StringComparison.Ordinal);
            Assert.Contains("collection may have stopped; get_collection_health shows which.", inside.Note, StringComparison.Ordinal);
            Assert.DoesNotContain("Retention removed it", inside.Note, StringComparison.Ordinal);

            /* A young store: its first row is five days old, so a window ten days back was never held. */
            await DeleteWaitRowsAsync(connection, ct);
            await PlantWaitAsync(connection, Naive(now.AddDays(-5)), StaleWait, 60_000L, ct);
            await PlantWaitAsync(connection, Naive(now.AddHours(-5)), StaleWait, 60_000L, ct);
            var young = await FactsAsync(10, 48);
            Assert.Contains("collection may have stopped; get_collection_health shows which.", young.Note, StringComparison.Ordinal);
            Assert.DoesNotContain("Retention removed it", young.Note, StringComparison.Ordinal);

            /* A stalled purge: the oldest row sits far before the horizon, so retention is not claimed. */
            await DeleteWaitRowsAsync(connection, ct);
            await PlantWaitAsync(connection, Naive(now.AddDays(-(retentionDays + 10))), StaleWait, 60_000L, ct);
            await PlantWaitAsync(connection, Naive(now.AddHours(-5)), StaleWait, 60_000L, ct);
            var stalled = await FactsAsync(retentionDays + 20, 48);
            Assert.Contains("collection may have stopped; get_collection_health shows which.", stalled.Note, StringComparison.Ordinal);
            Assert.DoesNotContain("Retention removed it", stalled.Note, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #9218: a replay whose whole window ends before the oldest row the store keeps is answered from one lookup of that row, and
    /// no fact statement runs, in <c>analyze_server</c> and in <c>get_analysis_facts</c>. A window that straddles the oldest
    /// row, and one inside retention, still run them (the same statements, counted through the Npgsql command log) and answer
    /// with facts, in the order the service scores them.
    /// </summary>
    [Fact]
    public async Task AWindowWhollyBeforeRetention_RunsNoFactStatement_AndAWindowThatReachesTheFloorOrSitsInsideStillReadsFacts()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live window-empty analysis test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var counter = new CommandCountingLoggerFactory();
        await using var postgres = new NpgsqlDataSourceBuilder(cs!).UseLoggerFactory(counter).Build();

        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(connection, ct);
            var service = new DarlingAnalysisService(postgres);

            int retentionDays;
            using (var days = new NpgsqlCommand("SELECT " + DarlingRetentionHorizons.FleetRetentionDaysSql("wait_stats"), connection))
                retentionDays = Convert.ToInt32(await days.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);

            var now = DateTime.UtcNow;
            string Iso(DateTime at) => at.ToString("o", System.Globalization.CultureInfo.InvariantCulture);

            /* The store as retention leaves it: its oldest row a day past the horizon, an hourly reading for thirty hours from there
               (so a window ending a day later has a day of history behind it and a collector that was up), and a quarter-hourly
               stretch ending now for the window inside retention. */
            var oldest = now.AddDays(-(retentionDays + 1));
            for (var hours = 0; hours <= 30; hours++)
                await PlantWaitAsync(connection, Naive(oldest.AddHours(hours)), BenignWait, 100L, ct);
            for (var minutesAgo = 600; minutesAgo >= 0; minutesAgo -= 15)
                await PlantWaitAsync(connection, Naive(now.AddMinutes(-minutesAgo)), BenignWait, 100L, ct);

            /* What a fact statement is: a read of one of the tables the facts are built from (config, trace flags, memory, file I/O,
               perfmon, query stats). The history gate, the retention lookup and the server lookup read none of them, and the same
               marker finds them on the windows below that do run the facts. */
            int FactReads() => new[] { "FROM server_config", "FROM trace_flags", "FROM memory_stats", "file_io_stats", "FROM perfmon_stats", "FROM query_stats" }
                .Sum(table => counter.Provider.CountContaining(table));

            /* ── wholly before retention: both tools answer from the lookup, and no fact statement runs. */
            var farBefore = Iso(now.AddDays(-(retentionDays + 10)));
            counter.Provider.Reset();
            var removed = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, hours_back: 4, as_of: farBefore);
            var removedFacts = await DarlingMcpTools.GetAnalysisFacts(service, postgres, ServerName, hours_back: 4, as_of: farBefore);
            Assert.Equal(0, FactReads());
            foreach (var answer in new[] { removed, removedFacts })
            {
                using var doc = JsonDocument.Parse(answer);
                Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
                var message = doc.RootElement.GetProperty("message").GetString()!;
                Assert.Contains("This window ends before the oldest row this store keeps (", message, StringComparison.Ordinal);
                Assert.Contains($"{retentionDays} days of retention for wait_stats", message, StringComparison.Ordinal);
                Assert.Contains("Retention removed it, and collection did not break.", message, StringComparison.Ordinal);
                Assert.DoesNotContain("get_collection_health", message, StringComparison.Ordinal);
            }
            Assert.True(service.WindowEmptyRemovedByRetention);

            /* ── a window that reaches the floor: it starts before the oldest row and ends a day after it, so it keeps its path. */
            var straddleEnd = Iso(oldest.AddHours(26));
            counter.Provider.Reset();
            var straddle = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, hours_back: 48, as_of: straddleEnd);
            Assert.True(FactReads() > 0);
            Assert.False(service.WindowEmptyRemovedByRetention);
            using (var doc = JsonDocument.Parse(straddle))
                Assert.NotEqual("unavailable", doc.RootElement.GetProperty("status").GetString());

            counter.Provider.Reset();
            var straddleFacts = await DarlingMcpTools.GetAnalysisFacts(service, postgres, ServerName, hours_back: 48, as_of: straddleEnd);
            Assert.True(FactReads() > 0);
            using (var doc = JsonDocument.Parse(straddleFacts))
                Assert.True(doc.RootElement.TryGetProperty("facts", out _));

            /* ── a window inside retention: the facts the tool returns are the ones the service scores, in severity order. */
            var insideEnd = now.AddMinutes(-30);
            counter.Provider.Reset();
            var insideJson = await DarlingMcpTools.GetAnalysisFacts(service, postgres, ServerName, hours_back: 4, as_of: Iso(insideEnd));
            Assert.True(FactReads() > 0);
            var (scored, _, _) = await service.CollectAndScoreFactsAsync(ServerId, ServerName, 4, DateTime.SpecifyKind(insideEnd, DateTimeKind.Utc), ct);
            Assert.NotEmpty(scored);
            using (var doc = JsonDocument.Parse(insideJson))
            {
                Assert.True(doc.RootElement.TryGetProperty("facts", out _));
                var returned = doc.RootElement.GetProperty("facts").EnumerateArray()
                    .Select(f => f.GetProperty("key").GetString()!).ToList();
                var expected = scored.OrderByDescending(f => f.Severity).Select(f => f.Key).ToList();
                Assert.Equal(expected, returned);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>Naive-UTC, the Kind every timestamp column in this store is bound with.</summary>
    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    private static async Task RegisterServerAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, 15, $4, $4)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE, sql_major_version = 15;", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(Naive(DateTime.UtcNow));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantWaitAsync(
        NpgsqlConnection connection, DateTime at, string waitType, long waitMs, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(waitType);
        command.Parameters.AddWithValue(50L);
        command.Parameters.AddWithValue(waitMs);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteWaitRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM wait_stats WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM wait_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
