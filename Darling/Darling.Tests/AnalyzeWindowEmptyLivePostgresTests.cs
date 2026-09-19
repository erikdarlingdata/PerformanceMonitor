/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
            var (facts, coverage) = await service.CollectAndScoreFactsAsync(ServerId, ServerName);
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
            Assert.Contains("but emitted no fact over it — the collector observed 100% of this 4h window", lines, StringComparison.Ordinal);
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
