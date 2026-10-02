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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Session Stats tab's read + chart mapping against the Darling store contract (no live Postgres).
/// The tab is the Dashboard-parity port of ResourceMetricsContent's Session Stats sub-tab; its feed is the
/// server-wide <c>v_session_summary_stats</c> view (the <see cref="SessionSummaryStatsCollector"/> that is
/// the 1:1 port of the Dashboard's <c>session_stats</c> table), NOT the per-application
/// <c>v_session_stats</c> FinOps' Application Connections read uses. These tests verify that every one of the
/// Dashboard's seven chart series + three summary fields maps to a column that actually exists in that
/// table, that the read is both-sides window-bound, and that the pure series/summary helpers shape the data
/// exactly as the Dashboard does.
/// </summary>
public sealed class ViewerSessionStatsSqlTests
{
    [Fact]
    public void SessionStatsSql_ReadsSessionSummaryView_BothSidesWindow_ThenBucketed_OrderedOldestBucketFirst()
    {
        var sql = ViewerDataService.SessionStatsSql;

        /* The server-wide SUMMARY view, not the per-application v_session_stats. */
        Assert.Contains("FROM v_session_summary_stats", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("FROM v_session_stats", sql, StringComparison.Ordinal);

        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);

        /* #4234/#4349: bucketed — the outer read joins agg + latest on bucket_start and orders by
         * the bucket, not by raw collection_time. */
        Assert.Contains("AS bucket_start", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY agg.bucket_start", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void SessionStatsSql_SelectsEveryDashboardSeriesAndSummaryColumn()
    {
        var sql = ViewerDataService.SessionStatsSql;

        /* The seven chart series columns + the three summary columns (+ the two connection counts) — every
           field the Dashboard's SessionStatsItem carries, so no series is silently dropped. */
        foreach (var column in new[]
        {
            "collection_time",
            "total_sessions",
            "running_sessions",
            "sleeping_sessions",
            "background_sessions",
            "dormant_sessions",
            "idle_sessions_over_30min",
            "sessions_waiting_for_memory",
            "databases_with_connections",
            "top_application_name",
            "top_application_connections",
            "top_host_name",
            "top_host_connections",
        })
        {
            Assert.Contains(column, sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void SessionStatsSql_PgDialect_PositionalParams_NoBareNow_NoNLiterals()
    {
        var sql = ViewerDataService.SessionStatsSql;

        Assert.DoesNotContain("now(", sql.ToLowerInvariant());
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.Contains("$1", sql, StringComparison.Ordinal);
        Assert.Contains("$2", sql, StringComparison.Ordinal);
        Assert.Contains("$3", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void SessionStatsSql_ReadsColumnsThatExistInTheGeneratedSessionSummaryTable()
    {
        /* The verified-columns pin: every column the read selects is a real column of the collector's
           generated table, so a series can never reference a column the store does not have. */
        Assert.Equal("session_summary_stats", SessionSummaryStatsCollector.Instance.TargetTable);

        var ddl = PgSchemaGenerator.CreateTable(SessionSummaryStatsCollector.Instance);
        foreach (var column in new[]
        {
            "collection_time", "total_sessions", "running_sessions", "sleeping_sessions",
            "background_sessions", "dormant_sessions", "idle_sessions_over_30min",
            "sessions_waiting_for_memory", "databases_with_connections", "top_application_name",
            "top_application_connections", "top_host_name", "top_host_connections",
        })
        {
            Assert.Contains(column, ddl, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void SessionSummaryView_IsPinnedInTheAuthoritativeViewList()
    {
        /* The tab reads the v_ passthrough view; it must be in the store's authoritative view set (so V14
           refreshes it and the migration test cross-checks it). */
        Assert.Contains("v_session_summary_stats", PgSchemaGenerator.AllPassthroughViews);
    }
}

/// <summary>
/// Pins the Session Stats chart's pure series mapping + summary formatting (no WpfPlot): the seven series
/// and their shared-palette colors, and the "name (count)" / N/A summary shaping — the same no-WPF
/// discipline as the rest of Darling.Tests. The render loop and the summary setter both consume these, so
/// the pins guard the exact Dashboard-parity mapping.
/// </summary>
public sealed class ViewerSessionStatsMappingTests
{
    [Fact]
    public void SessionSeriesSpecs_AreTheSevenDashboardSeries_InOrder()
    {
        var legends = SessionStatsChartRenderer.SessionSeriesSpecs.Select(s => s.Legend).ToList();

        Assert.Equal(
            new[] { "Total", "Running", "Sleeping", "Background", "Dormant", "Idle >30m", "Waiting for Memory" },
            legends);
    }

    [Fact]
    public void SessionSeriesSpecs_EachSeriesResolvesToADistinctSharedPaletteColor()
    {
        var keys = SessionStatsChartRenderer.SessionSeriesSpecs.Select(s => s.PaletteKey).ToList();

        Assert.Equal(
            new[] { "SessionTotal", "SessionRunning", "SessionSleeping", "SessionBackground", "SessionDormant", "SessionIdle", "SessionWaiting" },
            keys);

        /* Every key resolves through the shared palette to a valid #RRGGBB (so a typo'd key that fell back
           to a cycling color would still be a hex, but the exact-key list above pins the intended mapping). */
        var colors = keys.Select(ChartPalette.SeriesColor).ToList();
        Assert.All(colors, c => Assert.Matches("^#[0-9A-Fa-f]{6}$", c));

        /* The seven status series must be visually distinguishable — distinct colors. */
        Assert.Equal(keys.Count, colors.Distinct(StringComparer.OrdinalIgnoreCase).Count());
    }

    [Fact]
    public void SessionSeriesSpecs_Selectors_ReadTheMatchingStatusColumn()
    {
        /* Distinct value per column so a mis-wired selector (e.g. Running reading Sleeping) fails. */
        var point = new SessionStatsPoint(
            CollectionTime: DateTime.UtcNow,
            TotalSessions: 100,
            RunningSessions: 11,
            SleepingSessions: 22,
            BackgroundSessions: 33,
            DormantSessions: 44,
            IdleSessionsOver30Min: 55,
            SessionsWaitingForMemory: 66,
            DatabasesWithConnections: 7,
            TopApplicationName: "AppA",
            TopApplicationConnections: 40,
            TopHostName: "Host1",
            TopHostConnections: 30);

        var byLegend = SessionStatsChartRenderer.SessionSeriesSpecs.ToDictionary(s => s.Legend, s => s.Value(point));

        Assert.Equal(100, byLegend["Total"]);
        Assert.Equal(11, byLegend["Running"]);
        Assert.Equal(22, byLegend["Sleeping"]);
        Assert.Equal(33, byLegend["Background"]);
        Assert.Equal(44, byLegend["Dormant"]);
        Assert.Equal(55, byLegend["Idle >30m"]);
        Assert.Equal(66, byLegend["Waiting for Memory"]);
    }

    [Fact]
    public void FormatSessionSummary_Null_YieldsNAForAllThree()
    {
        var (topApp, topHost, databases) = SessionStatsSummary.Format(null);

        Assert.Equal("N/A", topApp);
        Assert.Equal("N/A", topHost);
        Assert.Equal("N/A", databases);
    }

    [Fact]
    public void FormatSessionSummary_FullPoint_RendersNameAndConnectionCount()
    {
        var point = Point(topApp: "SQLAgent", topAppConns: 42, topHost: "APPSRV01", topHostConns: 18, dbs: 9);

        var (topApp, topHost, databases) = SessionStatsSummary.Format(point);

        Assert.Equal("SQLAgent (42)", topApp);
        Assert.Equal("APPSRV01 (18)", topHost);
        Assert.Equal("9", databases);
    }

    [Fact]
    public void FormatSessionSummary_MissingApplicationOrHost_YieldsNA_ButKeepsDatabaseCount()
    {
        var point = Point(topApp: null, topAppConns: null, topHost: "", topHostConns: null, dbs: 3);

        var (topApp, topHost, databases) = SessionStatsSummary.Format(point);

        Assert.Equal("N/A", topApp);
        Assert.Equal("N/A", topHost);
        Assert.Equal("3", databases);
    }

    private static SessionStatsPoint Point(string? topApp, int? topAppConns, string? topHost, int? topHostConns, int dbs) =>
        new(
            CollectionTime: DateTime.UtcNow,
            TotalSessions: 0,
            RunningSessions: 0,
            SleepingSessions: 0,
            BackgroundSessions: 0,
            DormantSessions: 0,
            IdleSessionsOver30Min: 0,
            SessionsWaitingForMemory: 0,
            DatabasesWithConnections: dbs,
            TopApplicationName: topApp,
            TopApplicationConnections: topAppConns,
            TopHostName: topHost,
            TopHostConnections: topHostConns);
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trip for the Session Stats read: two in-window snapshots plus one
/// out-of-window snapshot prove the oldest-first ordering, the inclusive both-sides window bound, and the
/// latest-snapshot summary attribution. Shares the serialized "live-postgres" collection; uses a negative
/// sentinel server_id and cleans up in finally.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerSessionStatsLivePostgresTests
{
    private const int SessionServerId = -940003;
    private const string SessionServerName = "viewer-session-summary-e2e";

    [Fact]
    public async Task SessionStats_OrderedInWindow_ExcludesOutOfWindow_LatestSummary_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live session-stats test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteAsync(connection, SessionServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var t1 = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-10));
            var t2 = t1.AddMinutes(5);
            var tOut = t2.AddMinutes(10); // AFTER the window end below — must be excluded

            await InsertAsync(connection, 1, t1, total: 100, running: 5, sleeping: 90, background: 3, dormant: 2,
                idle: 10, waiting: 1, dbs: 7, topApp: "AppA", topAppConns: 40, topHost: "Host1", topHostConns: 30);
            await InsertAsync(connection, 2, t2, total: 120, running: 8, sleeping: 105, background: 4, dormant: 3,
                idle: 15, waiting: 2, dbs: 9, topApp: "AppB", topAppConns: 55, topHost: "Host2", topHostConns: 45);
            await InsertAsync(connection, 3, tOut, total: 999, running: 0, sleeping: 0, background: 0, dormant: 0,
                idle: 0, waiting: 0, dbs: 0, topApp: "OutOfWindow", topAppConns: 1, topHost: "OutHost", topHostConns: 1);

            var window = await viewer.GetSessionStatsAsync(SessionServerId, t1.AddMinutes(-1), t2.AddMinutes(1));

            /* Both-sides bound: only the two in-window rows, oldest-first. */
            Assert.Equal(2, window.Count);
            Assert.Equal(t1, window[0].CollectionTime);
            Assert.Equal(t2, window[1].CollectionTime);
            Assert.DoesNotContain(window, p => p.TotalSessions == 999);

            /* Column mapping on the latest snapshot. */
            var latest = window[^1];
            Assert.Equal(120, latest.TotalSessions);
            Assert.Equal(8, latest.RunningSessions);
            Assert.Equal(105, latest.SleepingSessions);
            Assert.Equal(4, latest.BackgroundSessions);
            Assert.Equal(3, latest.DormantSessions);
            Assert.Equal(15, latest.IdleSessionsOver30Min);
            Assert.Equal(2, latest.SessionsWaitingForMemory);
            Assert.Equal(9, latest.DatabasesWithConnections);
            Assert.Equal("AppB", latest.TopApplicationName);
            Assert.Equal(55, latest.TopApplicationConnections);
            Assert.Equal("Host2", latest.TopHostName);
            Assert.Equal(45, latest.TopHostConnections);

            /* The summary strip reads that latest snapshot. */
            var (topApp, topHost, databases) = SessionStatsSummary.Format(latest);
            Assert.Equal("AppB (55)", topApp);
            Assert.Equal("Host2 (45)", topHost);
            Assert.Equal("9", databases);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteAsync(cleanup, SessionServerId, cleanupCt));
        }
    }

    private static async Task InsertAsync(
        NpgsqlConnection connection, long collectionId, DateTime collectionTimeUtc,
        int total, int running, int sleeping, int background, int dormant, int idle, int waiting, int dbs,
        string topApp, int topAppConns, string topHost, int topHostConns)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO session_summary_stats
    (collection_id, collection_time, server_id, server_name,
     total_sessions, running_sessions, sleeping_sessions, background_sessions, dormant_sessions,
     idle_sessions_over_30min, sessions_waiting_for_memory, databases_with_connections,
     top_application_name, top_application_connections, top_host_name, top_host_connections)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)", connection);
        command.Parameters.AddWithValue(collectionId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(SessionServerId);
        command.Parameters.AddWithValue(SessionServerName);
        command.Parameters.AddWithValue(total);
        command.Parameters.AddWithValue(running);
        command.Parameters.AddWithValue(sleeping);
        command.Parameters.AddWithValue(background);
        command.Parameters.AddWithValue(dormant);
        command.Parameters.AddWithValue(idle);
        command.Parameters.AddWithValue(waiting);
        command.Parameters.AddWithValue(dbs);
        command.Parameters.AddWithValue(topApp);
        command.Parameters.AddWithValue(topAppConns);
        command.Parameters.AddWithValue(topHost);
        command.Parameters.AddWithValue(topHostConns);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static async Task DeleteAsync(NpgsqlConnection connection, int serverId, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM session_summary_stats WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
