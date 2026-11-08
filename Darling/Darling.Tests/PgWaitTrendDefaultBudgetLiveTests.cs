/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8319: <c>get_pg_wait_trend</c> sizes its points to the window and fits the MCP response budget, against a live store. On an
/// Aurora PostgreSQL target the engine_cumulative arm holds one row per collection, so a one-minute collector puts 1,440 rated
/// collections in the default 24-hour window; served one point per collection that answer is about 215 KB, far over
/// <see cref="McpResponseBudget.DefaultBytes"/>, with nothing in the reply saying why. The tool takes <c>bucket_minutes</c> like its
/// trend siblings, buckets both arms, and names the width it used; the web read of the same name shares the reader and buckets
/// to the chart budget.
/// </summary>
[Collection("live-postgres")]
public sealed class PgWaitTrendDefaultBudgetLiveTests
{
    private const string ServerName = "pg-wait-trend-default-budget";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string WaitEvent = "DataFileRead";

    /// <summary>The window: a fixed day ending at <see cref="AsOfText"/>, so the seeded minute grid and the bucket grid never depend on the clock.</summary>
    private static readonly DateTime WindowEnd = new(2026, 3, 6, 10, 0, 0);
    private const string AsOfText = "2026-03-06T10:00:00Z";
    private const int Hours = 24;
    private static readonly DateTime WindowStart = WindowEnd.AddHours(-Hours);

    /// <summary>One rated collection a minute across the whole window: 1,440 of them, each 60 completed waits and 3,000 ms of wait
    /// time over a 60-second interval, so every collection, and every bucket of any width, rates at 50 ms/s.</summary>
    private const int Collections = Hours * 60;
    private const long DeltaWaitsPerCollection = 60;
    private const long DeltaWaitTimeUsPerCollection = 3_000_000;
    private const int IntervalSeconds = 60;
    private const double SeededMsPerSecond = DeltaWaitTimeUsPerCollection / 1000.0 / IntervalSeconds;

    [Fact]
    public async Task TheDefaultCall_OnADayOfOneMinuteCollections_StaysUnderTheMcpResponseBudget_AndNamesItsBucket()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            await SeedAsync(connection, ct);

            var answer = await DarlingMcpPgTrendTools.GetPgWaitTrend(postgres, ServerName, WaitEvent, Hours, AsOfText, cancellationToken: ct);
            var root = Root(answer);
            Assert.Equal("wait_trend", root.GetProperty("status").GetString());
            Assert.Equal(PgWaitInstrument.EngineCumulative, root.GetProperty("instrument").GetString());

            var points = root.GetProperty("point_count").GetInt32();
            var bytes = Encoding.UTF8.GetByteCount(answer);
            TestContext.Current.TestOutputHelper?.WriteLine($"get_pg_wait_trend default: {points} points, {bytes / 1024.0:F1} KB");

            Assert.True(points > 0, "the default call returned no points");
            Assert.True(bytes <= McpResponseBudget.DefaultBytes,
                $"get_pg_wait_trend's default call is {bytes:N0} bytes, over the {McpResponseBudget.DefaultBytes:N0}-byte budget ({points} points)");
            Assert.True(points <= TrendBuckets.McpPointBudget,
                $"get_pg_wait_trend's default call served {points} points, over the {TrendBuckets.McpPointBudget}-point auto target");
            Assert.True(root.TryGetProperty("bucket_minutes", out var width) && width.GetInt32() >= 1,
                "the answer does not say which bucket width it chose");
            Assert.True(root.TryGetProperty("aggregate_note", out _), "the answer carries no aggregate_note");
            /* Every collection rated at the seeded figure, so every bucket does too. */
            Assert.All(root.GetProperty("points").EnumerateArray(),
                p => Assert.Equal(SeededMsPerSecond, p.GetProperty("estimated_wait_ms_per_second").GetDouble(), 3));
        });
    }

    [Fact]
    public async Task AnHourlyBucket_OverADayOfOneMinuteCollections_Answers24Points_AtTheSeededRate()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            await SeedAsync(connection, ct);

            var root = Root(await InvokeToolAsync(postgres, bucketMinutes: 60, ct));
            Assert.Equal("wait_trend", root.GetProperty("status").GetString());

            var points = root.GetProperty("points").EnumerateArray().ToArray();
            Assert.Equal(24, points.Length);
            Assert.Equal(24, root.GetProperty("point_count").GetInt32());
            Assert.Equal(60, root.GetProperty("bucket_minutes").GetInt32());
            Assert.Equal(Collections, root.GetProperty("interval_count").GetInt64());

            for (var i = 0; i < points.Length; i++)
            {
                /* Stamped at the bucket's start, the first at the window's start. */
                Assert.Equal(Stamp(WindowStart.AddHours(i)), Stamp(points[i].GetProperty("collection_time")));
                /* Counts summed across the hour's 60 collections; the rate recomputed from the summed wait time over the summed seconds. */
                Assert.Equal(60 * DeltaWaitsPerCollection, points[i].GetProperty("sample_count").GetInt64());
                Assert.Equal(SeededMsPerSecond, points[i].GetProperty("estimated_wait_ms_per_second").GetDouble(), 3);
                Assert.False(points[i].GetProperty("counter_reset").GetBoolean());
            }
        });
    }

    /// <summary>The web read of the same name shares the reader: it honours <c>bucket_minutes</c>, and left to itself it buckets to the
    /// chart's plot-sized ask rather than answering a point per collection.</summary>
    [Fact]
    public async Task TheWebRead_HonoursBucketMinutes_AndSizesItselfToThePlot()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            await SeedAsync(connection, ct);

            var hourly = Root(await ReadWebAsync(postgres, "bucket_minutes", "60"));
            Assert.Equal("wait_trend", hourly.GetProperty("status").GetString());
            Assert.Equal(24, hourly.GetProperty("point_count").GetInt32());
            Assert.Equal(60, hourly.GetProperty("bucket_minutes").GetInt32());

            var plot = Root(await ReadWebAsync(postgres, "points", "557"));
            Assert.Equal("wait_trend", plot.GetProperty("status").GetString());
            Assert.True(plot.TryGetProperty("bucket_minutes", out var width) && width.GetInt32() >= 1,
                "the web answer does not say which bucket width it chose");
            var points = plot.GetProperty("point_count").GetInt32();
            Assert.True(points > 0 && points <= 557, $"the web read served {points} points for a 557-point plot");
        });
    }

    /// <summary>Calls the MCP tool by its parameter names, so the call reads the same whether or not the tool takes <c>bucket_minutes</c>:
    /// a tool without the parameter is asked for the window and answers as it does by default.</summary>
    private static Task<string> InvokeToolAsync(NpgsqlDataSource postgres, int bucketMinutes, CancellationToken ct)
    {
        var method = typeof(DarlingMcpPgTrendTools).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nameof(DarlingMcpPgTrendTools.GetPgWaitTrend) && m.GetParameters().Any(p => p.ParameterType == typeof(NpgsqlDataSource)));
        var supplied = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["postgres"] = postgres,
            ["server_name"] = ServerName,
            ["wait_event"] = WaitEvent,
            ["hours_back"] = Hours,
            ["as_of"] = AsOfText,
            ["bucket_minutes"] = bucketMinutes,
            ["cancellationToken"] = ct,
        };
        var args = method.GetParameters()
            .Select(p => supplied.TryGetValue(p.Name!, out var value) ? value : p.HasDefaultValue ? p.DefaultValue : null)
            .ToArray();
        return McpReadToolBudgetLiveTests.InvokeAsync(method, args);
    }

    /// <summary>The web read with the window's parameters and one more, <paramref name="key"/>=<paramref name="value"/>.</summary>
    private static async Task<string> ReadWebAsync(NpgsqlDataSource postgres, string key, string value)
    {
        var query = new List<KeyValuePair<string, string?>>
        {
            new("server_name", ServerName),
            new("wait_event", WaitEvent),
            new("hours_back", Hours.ToString(CultureInfo.InvariantCulture)),
            new("as_of", AsOfText),
            new(key, value),
        };
        var context = new DefaultHttpContext();
        context.Request.QueryString = QueryString.Create(query);
        return await DarlingWebEndpoints.BuildReadDispatch()["get_pg_wait_trend"](context, postgres, null!);
    }

    /// <summary>The collector's rows for one event across the window: a rated collection every minute from the window's start, the
    /// last one a minute before its end, each carrying the same delta over the same 60-second interval.</summary>
    private static Task SeedAsync(NpgsqlConnection connection, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO pg_wait_stats (collection_id, collection_time, server_id, server_name, wait_type_id, wait_event_id, wait_type, wait_event, waits, wait_time_us, delta_waits, delta_wait_time_us, sample_interval_seconds) "
            + "SELECT $1 + g, $2 + g * INTERVAL '1 minute', $3, $4, 1, 101, 'IO', $5, 1000 + $6 * (g + 1), 50000000 + $7 * (g + 1), $6, $7, $8 "
            + "FROM generate_series(0, $9) AS g",
            CollectionIdGenerator.Next(), WindowStart, ServerId, ServerName, WaitEvent, DeltaWaitsPerCollection, DeltaWaitTimeUsPerCollection, IntervalSeconds, Collections - 1);

    private static async Task RunAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live PostgreSQL wait trend budget tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET engine_kind = $2 WHERE server_id = $1", ServerId, MonitoredEngineKind.AuroraPostgres);
            await body(connection, postgres, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in new[] { "pg_wait_stats", "pg_wait_sampling", "collector_state", "servers" })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM {table} WHERE server_id = $1", ServerId);
        }
    }

    private static string Stamp(DateTime value) => value.ToString("yyyy-MM-ddTHH:mm:ss", CultureInfo.InvariantCulture);

    private static string Stamp(JsonElement value) => value.GetString()![..19];

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement;
}
