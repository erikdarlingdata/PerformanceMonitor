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
using System.Threading.Tasks;
using Darling.Tests;
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #5244 PR4 (lane W4): the six duration-trend, Query Store clutter, long-query and plan-correction reads the web pages drive,
/// called the way the page calls them, through the web read dispatch with two REPEATED <c>database_name</c> keys over a seed of
/// databases A, B and C. Each read must answer with A's and B's rows and none of C's. On the base the dispatch ignored the key,
/// so each read answered with all three databases. The same class pins the PR's wording (a filtered empty answer says "for the
/// database X" for one name and "for the chosen databases" for two or more) and the <c>database_name</c> echo (blank is null).
/// </summary>
[Collection("live-postgres")]
public sealed class QueryReadsWebDispatchLiveTests
{
    private const string EventDbA = QueryEventsDatabaseFilterLiveTests.DbA;
    private const string EventDbB = QueryEventsDatabaseFilterLiveTests.DbB;
    private const string EventDbC = QueryEventsDatabaseFilterLiveTests.DbC;
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live query-reads web dispatch tests.";

    private static async Task<string> WebReadAsync(NpgsqlDataSource postgres, string serverName, string read, string hours, params string[] databases)
    {
        var query = new List<KeyValuePair<string, string?>> { new("server_name", serverName), new("hours_back", hours) };
        query.AddRange(databases.Select(d => new KeyValuePair<string, string?>("database_name", d)));
        var context = new DefaultHttpContext();
        context.Request.QueryString = QueryString.Create(query);
        return await DarlingWebEndpoints.BuildReadDispatch()[read](context, postgres, null!);
    }

    private static string? Echo(string json) =>
        JsonDocument.Parse(json).RootElement.TryGetProperty("database_name", out var echo) && echo.ValueKind == JsonValueKind.String ? echo.GetString() : null;

    private static double Rate(string json) => DurationTrendDatabaseFilterLiveTests.LastRate(json)!.Value;

    /// <summary>The three trend reads over one raw-tier seed: A 3,000, B 2,000 and C 9,000 weight, so A+B is 5,000 and all is 14,000.</summary>
    [Fact]
    public Task TheThreeTrendReads_TwoRepeatedKeys_ReadOnlyAAndBsWork() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("trend-web-dispatch-w4", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            const string A = DurationTrendDatabaseFilterLiveTests.A;
            const string B = DurationTrendDatabaseFilterLiveTests.B;
            const string C = DurationTrendDatabaseFilterLiveTests.C;
            foreach (var (db, weight) in new[] { (A, 3_000L), (B, 2_000L), (C, 9_000L) })
            {
                await DurationTrendDatabaseFilterLiveTests.PlantQueryAsync(connection, ct, serverId, serverName, now.AddHours(-2), db, "SELECT " + db, weight);
                await DurationTrendDatabaseFilterLiveTests.PlantProcedureAsync(connection, ct, serverId, serverName, now.AddHours(-2), db, weight);
                await DurationTrendDatabaseFilterLiveTests.PlantQueryStoreAsync(connection, ct, serverId, serverName, now.AddHours(-3), db, weight);
                await DurationTrendDatabaseFilterLiveTests.PlantQueryStoreAsync(connection, ct, serverId, serverName, now.AddHours(-2), db, weight);
            }

            const double perSecond = 1.0 / 1000 / 3600;
            const double storePerSecond = 10.0 / 1000 / 3600;
            foreach (var (read, unit) in new[]
                     {
                         ("get_query_duration_trend", perSecond), ("get_procedure_duration_trend", perSecond), ("get_query_store_duration_trend", storePerSecond),
                     })
            {
                var all = await WebReadAsync(postgres, serverName, read, "24");
                Assert.Equal(14_000 * unit, Rate(all), 9);
                Assert.Null(Echo(all));

                var both = await WebReadAsync(postgres, serverName, read, "24", A, B);
                Assert.True(Math.Abs(5_000 * unit - Rate(both)) < 1e-9, $"{read} over A and B read {Rate(both)}, not A + B's work: {both}");
                Assert.Equal("the chosen databases", Echo(both));

                var one = await WebReadAsync(postgres, serverName, read, "24", B);
                Assert.Equal(2_000 * unit, Rate(one), 9);
                Assert.Equal(B, Echo(one));
            }

            /* A blank name is every database and echoes null, never the raw whitespace. */
            var blank = await DarlingMcpTrendTools.GetQueryDurationTrend(postgres, serverName, 24, null, null, "   ", ct);
            Assert.Equal(14_000 * perSecond, Rate(blank), 9);
            Assert.Null(Echo(blank));
            Assert.Equal(14_000 * storePerSecond, Rate(await DarlingMcpTrendTools.GetQueryStoreDurationTrend(postgres, serverName, 24, null, "", ct)), 9);
        }, DurationTrendDatabaseFilterLiveTests.Cleanup);

    /// <summary>The empty answers of the three trends: one name by its name, two by "the chosen databases", status stays <c>empty</c>.</summary>
    [Fact]
    public Task TheTrendEmptyAnswers_NameTheDatabase_WithTheSharedSentenceForm() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("trend-empty-wording-w4", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            const string A = DurationTrendDatabaseFilterLiveTests.A;
            await DurationTrendDatabaseFilterLiveTests.PlantQueryAsync(connection, ct, serverId, serverName, now.AddHours(-30), A, "SELECT A", 3_000L);
            await DurationTrendDatabaseFilterLiveTests.PlantProcedureAsync(connection, ct, serverId, serverName, now.AddHours(-30), A, 3_000L);

            foreach (var read in new[] { "get_query_duration_trend", "get_procedure_duration_trend" })
            {
                var one = JsonDocument.Parse(await WebReadAsync(postgres, serverName, read, "2", "NoSuchDb")).RootElement;
                Assert.Equal("empty", one.GetProperty("status").GetString());
                Assert.Contains(" for the database NoSuchDb", one.GetProperty("message").GetString(), StringComparison.Ordinal);
                Assert.DoesNotContain("database_name '", one.GetProperty("message").GetString(), StringComparison.Ordinal);

                var many = JsonDocument.Parse(await WebReadAsync(postgres, serverName, read, "2", "NoX", "NoY")).RootElement;
                Assert.Equal("empty", many.GetProperty("status").GetString());
                Assert.Contains(" for the chosen databases", many.GetProperty("message").GetString(), StringComparison.Ordinal);
            }
        }, DurationTrendDatabaseFilterLiveTests.Cleanup);

    /// <summary>The three event and Query Store reads over the lane R2 seed: A, B and C each hold rows.</summary>
    [Fact]
    public async Task TheLongQueryPlanCorrectionAndClutterReads_TwoRepeatedKeys_ReturnOnlyAAndBsRows()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var serverName = QueryEventsDatabaseFilterLiveTests.ServerName;
        var bodySucceeded = false;
        try
        {
            await QueryEventsDatabaseFilterLiveTests.SeedAsync(cs!, ct);

            var longQueries = JsonDocument.Parse(await WebReadAsync(postgres, serverName, "get_long_query_completions", "1", EventDbA, EventDbB)).RootElement;
            Assert.Equal(new[] { EventDbA, EventDbB }, longQueries.GetProperty("completions").EnumerateArray()
                .Select(r => r.GetProperty("database_name").GetString()!).Distinct().Order().ToArray());
            Assert.Equal(3, longQueries.GetProperty("completions_returned").GetInt32());
            Assert.Equal("the chosen databases", longQueries.GetProperty("database_name").GetString());

            var corrections = JsonDocument.Parse(await WebReadAsync(postgres, serverName, "get_plan_corrections", "1", EventDbA, EventDbB)).RootElement;
            Assert.DoesNotContain(EventDbC, corrections.GetProperty("recommendations").EnumerateArray().Select(r => r.GetProperty("database_name").GetString()));
            Assert.Equal(new[] { EventDbA, EventDbB }, corrections.GetProperty("recommendations").EnumerateArray()
                .Select(r => r.GetProperty("database_name").GetString()!).Distinct().Order().ToArray());
            Assert.Equal("the chosen databases", corrections.GetProperty("database_name").GetString());

            var clutter = JsonDocument.Parse(await WebReadAsync(postgres, serverName, "get_query_store_clutter", "1", EventDbA, EventDbB)).RootElement;
            Assert.Equal(new[] { EventDbA, EventDbB }, clutter.GetProperty("databases").EnumerateArray()
                .Select(d => d.GetProperty("database_name").GetString()!).Order().ToArray());
            Assert.Equal("the chosen databases", clutter.GetProperty("database_name").GetString());

            /* The unfiltered page still holds all three (the control), and echoes null. */
            var all = JsonDocument.Parse(await WebReadAsync(postgres, serverName, "get_query_store_clutter", "1")).RootElement;
            Assert.Equal(3, all.GetProperty("databases").GetArrayLength());
            Assert.Null(Echo(all.GetRawText()));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await QueryEventsDatabaseFilterLiveTests.DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>A filtered empty answer says "for the database X" for one name and "for the chosen databases" for two or more, on all three event reads.</summary>
    [Fact]
    public async Task TheEventReadsFilteredEmptyAnswers_UseTheSharedSelectionWords()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var serverName = QueryEventsDatabaseFilterLiveTests.ServerName;
        var bodySucceeded = false;
        try
        {
            await QueryEventsDatabaseFilterLiveTests.SeedAsync(cs!, ct);

            foreach (var (read, phrase) in new[]
                     {
                         ("get_long_query_completions", "specified time range"),
                         ("get_plan_corrections", "No plan correction data found"),
                         ("get_query_store_clutter", "capture"),
                     })
            {
                var one = JsonDocument.Parse(await WebReadAsync(postgres, serverName, read, "1", "NoSuchDb")).RootElement;
                Assert.Equal("empty", one.GetProperty("status").GetString());
                var oneMessage = one.GetProperty("message").GetString()!;
                Assert.Contains(phrase, oneMessage, StringComparison.Ordinal);
                Assert.Contains(" for the database NoSuchDb", oneMessage, StringComparison.Ordinal);
                Assert.DoesNotContain("for the chosen databases", oneMessage, StringComparison.Ordinal);

                var many = JsonDocument.Parse(await WebReadAsync(postgres, serverName, read, "1", "NoX", "NoY")).RootElement;
                Assert.Equal("empty", many.GetProperty("status").GetString());
                Assert.Contains(" for the chosen databases", many.GetProperty("message").GetString(), StringComparison.Ordinal);
            }

            /* A blank name is every database: the public tool echoes null and answers with the rows. */
            var blank = await DarlingMcpLongQueryTools.GetLongQueryCompletions(postgres, serverName, 1, 30, null, "  ", null, ct);
            Assert.Null(Echo(blank));
            Assert.Equal(6, JsonDocument.Parse(blank).RootElement.GetProperty("completions_returned").GetInt32());
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await QueryEventsDatabaseFilterLiveTests.DeleteRowsAsync(cleanup, cleanupCt));
        }
    }
}
