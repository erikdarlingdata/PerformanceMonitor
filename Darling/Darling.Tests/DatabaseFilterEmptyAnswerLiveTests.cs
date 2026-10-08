/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #5244 PR4 review round 2 (L3 and L2), run against a store. L3: a long-query answer that a filter emptied says the
/// chosen databases had no long queries when the store holds long-query rows for the server in the window, and keeps the
/// opt-in sentence only when the store holds none. L2: every answer shape of the six tools that take <c>database_name</c>,
/// <c>not_collected</c> and <c>precondition</c> included, echoes it (the name for one database, "the chosen databases"
/// for two or more, null for all).
/// </summary>
[Collection("live-postgres")]
public sealed class DatabaseFilterEmptyAnswerLiveTests
{
    private const string Cleanup =
        "DELETE FROM long_query_completions WHERE server_id = {0}; DELETE FROM collection_log WHERE server_id = {0}; " +
        "DELETE FROM query_stats WHERE server_id = {0}; DELETE FROM procedure_stats WHERE server_id = {0}; " +
        "DELETE FROM query_store_stats WHERE server_id = {0}; DELETE FROM query_store_health WHERE server_id = {0}";

    private static string Message(string json) => JsonDocument.Parse(json).RootElement.GetProperty("message").GetString()!;

    private static string? Echo(string json)
    {
        var root = JsonDocument.Parse(json).RootElement;
        Assert.True(root.TryGetProperty("database_name", out var echo), "no database_name key in: " + json);
        return echo.ValueKind == JsonValueKind.Null ? null : echo.GetString();
    }

    [Fact]
    public Task LongQueryEmpty_UnderAFilter_BlamesTheOptInSwitch_OnlyWhenTheStoreHoldsNoLongQueryRows() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("a5244r2-lq-on", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            /* Another database's long query inside the window: the collector is on and has rows, so the chosen database's
               empty answer is about the database, never about a switch. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO long_query_completions (long_query_completion_id, collection_time, server_id, server_name, event_time, event_type, database_name, duration_microseconds, statement_text)
                  VALUES ($1, $2, $3, $4, $2, 'rpc_completed', 'DbOther', 5000000, 'EXEC p')",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now.AddMinutes(-30)), serverId, serverName);

            var withRows = await DarlingMcpLongQueryTools.GetLongQueryCompletions(
                postgres, serverName, 24, 10, null, DatabaseFilter.One("DbChosen"), null, ct);
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(withRows));
            Assert.Contains("DbChosen", Message(withRows), StringComparison.Ordinal);
            Assert.DoesNotContain("opt-in", Message(withRows), StringComparison.Ordinal);
            Assert.Equal("DbChosen", Echo(withRows));
        }, Cleanup);

    [Fact]
    public Task LongQueryEmpty_UnderAFilter_KeepsTheOptInSentence_WhenTheStoreHoldsNoLongQueryRows() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("a5244r2-lq-off", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            var filtered = await DarlingMcpLongQueryTools.GetLongQueryCompletions(
                postgres, serverName, 24, 10, null, DatabaseFilter.Of(new[] { "DbA", "DbB" }), null, ct);
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(filtered));
            Assert.Contains("opt-in", Message(filtered), StringComparison.Ordinal);
            Assert.Equal(DatabaseFilter.ManyDatabasesDescription, Echo(filtered));

            var unfiltered = await DarlingMcpLongQueryTools.GetLongQueryCompletions(
                postgres, serverName, 24, 10, null, DatabaseFilter.All, null, ct);
            Assert.Contains("opt-in", Message(unfiltered), StringComparison.Ordinal);
            Assert.Null(Echo(unfiltered));
        }, Cleanup);

    private static IEnumerable<(string Tool, Func<NpgsqlDataSource, string, DatabaseFilter, CancellationToken, Task<string>> Call)> SixTools()
    {
        var budget = TrendBudget.Mcp(TrendBuckets.DurationMaxPoints);
        yield return ("get_query_duration_trend", (p, s, d, ct) => DarlingMcpTrendTools.GetQueryDurationTrend(p, s, 24, null, null, d, budget, ct));
        yield return ("get_procedure_duration_trend", (p, s, d, ct) => DarlingMcpTrendTools.GetProcedureDurationTrend(p, s, 24, null, null, d, budget, ct));
        yield return ("get_query_store_duration_trend", (p, s, d, ct) => DarlingMcpTrendTools.GetQueryStoreDurationTrend(p, s, 24, null, d, ct));
        yield return ("get_long_query_completions", (p, s, d, ct) => DarlingMcpLongQueryTools.GetLongQueryCompletions(p, s, 24, 10, null, d, null, ct));
        yield return ("get_plan_corrections", (p, s, d, ct) => DarlingMcpPlanCorrectionTools.GetPlanCorrections(p, s, 24, 10, null, false, d, null, ct));
        yield return ("get_query_store_clutter", (p, s, d, ct) => DarlingMcpQueryStoreClutterTools.GetQueryStoreClutter(p, s, 24, 10, false, null, d, ct));
    }

    /// <summary>
    /// L2, <c>not_collected</c>: a server whose engine cannot run the collector answers with that status from every one of
    /// the six tools, and each says which databases the call was limited to. One database gives the name, two or more give
    /// "the chosen databases", every database gives null.
    /// </summary>
    [Fact]
    public Task NotCollected_OnEveryToolThatTakesADatabase_EchoesTheDatabase() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("a5244r2-not-collected", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET engine_kind = $2 WHERE server_id = $1", serverId, MonitoredEngineKind.Postgres);

            foreach (var (tool, call) in SixTools())
            {
                var one = await call(postgres, serverName, DatabaseFilter.One("DbA"), ct);
                Assert.True(DarlingMcpTestData.StatusOf(one) == "not_collected", tool + ": " + one);
                Assert.True(Echo(one) == "DbA", tool + " (one database)");

                var two = await call(postgres, serverName, DatabaseFilter.Of(new[] { "DbA", "DbB" }), ct);
                Assert.True(DarlingMcpTestData.StatusOf(two) == "not_collected", tool);
                Assert.True(Echo(two) == DatabaseFilter.ManyDatabasesDescription, tool + " (two databases)");

                var all = await call(postgres, serverName, DatabaseFilter.All, ct);
                Assert.True(DarlingMcpTestData.StatusOf(all) == "not_collected", tool);
                Assert.True(Echo(all) is null, tool + " (all databases)");
            }
        }, Cleanup);

    /// <summary>
    /// L2, <c>precondition</c>: the collector is on but its last run recorded a missing capture session, and the two tools
    /// that read that outcome (long queries, Query Store clutter) answer <c>precondition</c> with the echo.
    /// </summary>
    [Fact]
    public Task Precondition_OnTheToolsThatReadTheCollectorOutcome_EchoesTheDatabase() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("a5244r2-precondition", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            var collectors = new[] { "long_query_completions", DarlingQueryStoreClutterReader.CollectorName };
            foreach (var collector in collectors)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES ($1, $2, $3, $4, $5, $6)",
                    CollectionIdGenerator.Next(), serverId, serverName, collector, DarlingMcpTestData.Naive(now.AddMinutes(-5)),
                    CollectorRuntimePrecondition.CaptureSessionMissingStatus);
            }

            foreach (var (tool, call) in SixTools())
            {
                if (tool is not ("get_long_query_completions" or "get_query_store_clutter"))
                {
                    continue;
                }

                var one = await call(postgres, serverName, DatabaseFilter.One("DbA"), ct);
                Assert.True(DarlingMcpTestData.StatusOf(one) == CollectorRuntimePrecondition.StatusWord, tool + ": " + one);
                Assert.True(Echo(one) == "DbA", tool + " (one database)");

                var two = await call(postgres, serverName, DatabaseFilter.Of(new[] { "DbA", "DbB" }), ct);
                Assert.True(Echo(two) == DatabaseFilter.ManyDatabasesDescription, tool + " (two databases)");

                var all = await call(postgres, serverName, DatabaseFilter.All, ct);
                Assert.True(DarlingMcpTestData.StatusOf(all) == CollectorRuntimePrecondition.StatusWord, tool);
                Assert.True(Echo(all) is null, tool + " (all databases)");
            }
        }, Cleanup);
}
