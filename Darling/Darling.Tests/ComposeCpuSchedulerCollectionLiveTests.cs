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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Custom Views scheduler measures read ONE combined row per (server, collection_time) (#5710). A collection
/// stored before the collector fix is split across rows that each cover some schedulers (83 + 18 runnable tasks for a
/// 101-task reading); a panel that averaged the raw rows drew 50.5, and its Max or Min drew a piece. The relation the
/// compiler reads is the shared per-collection combine, so the time aggregate runs over collections.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to CREATE
   and DROP its own database through ScratchPostgres, then works entirely inside it. */
public sealed class ComposeCpuSchedulerCollectionLiveTests
{
    private const int ServerId1 = -571001;
    private const int ServerId2 = -571002;
    private const string ServerName1 = "cpu-sched-split";
    private const string ServerName2 = "cpu-sched-other";

    private static readonly DateTime WindowStart = new(2026, 8, 1, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowEnd = WindowStart.AddHours(3);

    /* The second server's row shares c1's collection_time, so a combine that groups by collection_time alone (not by server) would add it
       to c1 (10 + 30). */
    private static readonly DateTime C1 = WindowStart.AddMinutes(10);   // hour 0, one row: 10 tasks, 20 %
    private static readonly DateTime C2 = WindowStart.AddMinutes(40);   // hour 0, 3 + 1 split: 83 + 18 = 101 tasks, 40 %
    private static readonly DateTime C3 = WindowStart.AddMinutes(70);   // hour 1, all-zero split: 0 + 0 + 0 + 0
    private static readonly DateTime C4 = WindowStart.AddMinutes(100);  // hour 1, 2-way split: 5 + 3 = 8 tasks, 10 %

    [Fact]
    public void BothSchedulerMeasures_CompileOverTheSharedPerCollectionCombine()
    {
        foreach (var measure in new[] { "scheduler_runnable_pct", "scheduler_runnable_tasks" })
        {
            var sql = CompileSql(Plan(measure, "avg"));
            Assert.Contains("GROUP BY server_id, server_name, collection_time)", sql, StringComparison.Ordinal);
            Assert.Contains(PerformanceMonitor.Common.CpuSchedulerCollectionSql.CombinedColumns, sql, StringComparison.Ordinal);
            Assert.Contains("FROM collect.cpu_scheduler_stats WHERE collection_time >= $1 AND collection_time <= $2 GROUP BY", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void EveryCatalogMeasureOnTheSource_ReadsAColumnTheCombineOutputs()
    {
        /* The compiler reads the combine's output, not the table: a measure on a column the combine does not name would fail at run time. */
        var measures = MeasureCatalog.Measures.Where(m => m.SourceTable == "cpu_scheduler_stats").ToList();
        Assert.Equal(new[] { "scheduler_runnable_pct", "scheduler_runnable_tasks" }, measures.Select(m => m.Key).OrderBy(k => k, StringComparer.Ordinal));
        foreach (var measure in measures)
        {
            Assert.Contains($" AS {measure.Column}", PerformanceMonitor.Common.CpuSchedulerCollectionSql.CombinedColumns, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task SplitCollections_AreOneValue_PerBucket_ForEveryAggregate_AgainstDevPostgres()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5710 compose live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId1, ServerName1, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId2, ServerName2, ct);

        long id = 1;
        async Task InsertAsync(int serverId, string serverName, DateTime time, int tasks, decimal pct)
        {
            await using var command = new NpgsqlCommand(
                """
                INSERT INTO collect.cpu_scheduler_stats
                    (collection_id, collection_time, server_id, server_name, max_workers_count, scheduler_count, cpu_count,
                     total_runnable_tasks_count, total_work_queue_count, total_current_workers_count, avg_runnable_tasks_count,
                     runnable_percent)
                VALUES ($1, $2, $3, $4, 960, 4, 4, $5, 0, 20, 0, $6)
                """, connection);
            command.Parameters.AddWithValue(id++);
            command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, time);
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(serverName);
            command.Parameters.AddWithValue(tasks);
            command.Parameters.AddWithValue(pct);
            await command.ExecuteNonQueryAsync(ct);
        }

        /* c1: the exact single-row collection */
        await InsertAsync(ServerId1, ServerName1, C1, 10, 20m);
        await InsertAsync(ServerId2, ServerName2, C1, 30, 50m);
        /* c2: a 3 + 1 split; every piece repeats the workload-group percent */
        await InsertAsync(ServerId1, ServerName1, C2, 40, 40m);
        await InsertAsync(ServerId1, ServerName1, C2, 23, 40m);
        await InsertAsync(ServerId1, ServerName1, C2, 20, 40m);
        await InsertAsync(ServerId1, ServerName1, C2, 18, 40m);
        /* c3: an all-zero split */
        await InsertAsync(ServerId1, ServerName1, C3, 0, 0m);
        await InsertAsync(ServerId1, ServerName1, C3, 0, 0m);
        /* c4: a two-way split */
        await InsertAsync(ServerId1, ServerName1, C4, 5, 10m);
        await InsertAsync(ServerId1, ServerName1, C4, 3, 10m);

        var one = new[] { ServerName1 };
        var h0 = WindowStart;
        var h1 = WindowStart.AddHours(1);

        /* Runnable tasks per collection: c1 10, c2 101, c3 0, c4 8. Raw rows would give hour 0 an Avg of 22.2 (five rows) and a Min of 10, a Max of 40. */
        await AssertAsync(connection, "scheduler_runnable_tasks", "avg", one, (h0, 55.5), (h1, 4.0));
        await AssertAsync(connection, "scheduler_runnable_tasks", "max", one, (h0, 101.0), (h1, 8.0));
        await AssertAsync(connection, "scheduler_runnable_tasks", "min", one, (h0, 10.0), (h1, 0.0));
        /* The percent is a repeated column, so a piece carries the collection's value: per collection c1 20, c2 40, c3 0, c4 10. */
        await AssertAsync(connection, "scheduler_runnable_pct", "avg", one, (h0, 30.0), (h1, 5.0));
        await AssertAsync(connection, "scheduler_runnable_pct", "max", one, (h0, 40.0), (h1, 10.0));
        await AssertAsync(connection, "scheduler_runnable_pct", "min", one, (h0, 20.0), (h1, 0.0));
        /* All servers: c1 of the other server is its own collection (30 tasks, 50 %), so hour 0 averages three collections. */
        await AssertAsync(connection, "scheduler_runnable_tasks", "avg", null, (h0, 47.0), (h1, 4.0));
        await AssertAsync(connection, "scheduler_runnable_pct", "max", null, (h0, 50.0), (h1, 10.0));

    }

    private static async Task AssertAsync(
        NpgsqlConnection connection, string measure, string aggregate, string[]? servers, params (DateTime Bucket, double Value)[] expected)
    {
        var ct = TestContext.Current.CancellationToken;
        var plan = Plan(measure, aggregate);
        var context = new ComposeRunContext(servers, WindowStart, WindowEnd, ComposeRunContext.NoVariables, RollupAvailability.None, WindowEnd, RollupCoverage.Unknown);
        var (compiled, error) = ComposeCompiler.Compile(plan, context);
        Assert.True(error is null, error);

        var actual = new Dictionary<DateTime, double>();
        await using var command = new NpgsqlCommand(compiled!.Sql, connection);
        foreach (var p in compiled.Parameters)
        {
            command.Parameters.Add(p);
        }

        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            actual[DateTime.SpecifyKind(reader.GetDateTime(reader.GetOrdinal("bucket")), DateTimeKind.Unspecified)] =
                Convert.ToDouble(reader.GetValue(reader.GetOrdinal("value")), CultureInfo.InvariantCulture);
        }

        var label = $"{measure} {aggregate} ({(servers is null ? "all servers" : "one server")})";
        Assert.Equal(expected.Length, actual.Count);
        foreach (var (bucket, value) in expected)
        {
            Assert.True(actual.TryGetValue(bucket, out var got), $"{label}: no row for bucket {bucket:O}");
            Assert.True(Math.Abs(got - value) < 0.0001, $"{label}: bucket {bucket:O} = {got}, expected {value}");
        }
    }

    private static string CompileSql(PanelPlan plan)
    {
        var context = new ComposeRunContext(null, WindowStart, WindowEnd, ComposeRunContext.NoVariables, RollupAvailability.None, WindowEnd, RollupCoverage.Unknown);
        var (compiled, error) = ComposeCompiler.Compile(plan, context);
        Assert.True(error is null, error);
        return compiled!.Sql;
    }

    private static PanelPlan Plan(string measure, string aggregate)
    {
        var json = "{\"source\":\"cpu_scheduler_stats\",\"measure\":\"" + measure + "\",\"aggregate\":\"" + aggregate + "\",\"timeBucket\":\"hour\",\"viz\":\"line\"}";
        var (plan, error) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(json)!, Array.Empty<string>());
        Assert.True(error is null, error);
        return plan!;
    }
}
