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
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test mints its own scratch store database through ScratchPostgres. */

/// <summary>
/// get_store_pass_steps against a store holding step rows (#9411): the window, newest first, the pass and step filters, the row cap with
/// the matching total, the NULL counts, a store without the table, and an empty window.
/// </summary>
[Collection("live-postgres")]
public sealed class StorePassStepsToolLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task WithStoreAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live #9411 pass step tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await body(connection, postgres, ct);
    }

    private static Task SeedAsync(
        NpgsqlConnection connection, CancellationToken ct, string pass, string step, DateTime started, double seconds,
        string outcome = StorePassSteps.OutcomeOk, long? built = null, long? removed = null, string? detail = null) =>
        DarlingMcpTestData.ExecAsync(connection, ct, StorePassSteps.InsertSql,
            pass, step, DarlingMcpTestData.Naive(started), DarlingMcpTestData.Naive(started.AddSeconds(seconds)), outcome, built, removed, detail);

    private static async Task<JsonElement> ReadAsync(
        NpgsqlDataSource postgres, CancellationToken ct, int hours = 24, string? pass = null, string? step = null, int limit = 50, string? asOf = null)
    {
        var json = await DarlingMcpStorePassStepsTools.GetStorePassSteps(postgres, hours, pass, step, limit, asOf, ct);
        Assert.False(McpHelpers.IsErrorEnvelope(json), json[..Math.Min(json.Length, 400)]);
        return JsonDocument.Parse(json).RootElement.Clone();
    }

    private static string[] Steps(JsonElement answer) =>
        answer.GetProperty("steps").EnumerateArray().Select(s => s.GetProperty("step").GetString()!).ToArray();

    [Fact]
    public async Task Window_KeepsStepsStartedInsideIt_NewestFirst_WithDurationAndNullCounts()
    {
        await WithStoreAsync(async (connection, postgres, ct) =>
        {
            var now = DateTime.UtcNow;
            await SeedAsync(connection, ct, StorePassSteps.StoreMaintenancePass, "oldest_inside", now.AddHours(-20), 5, built: 7, removed: 2);
            await SeedAsync(connection, ct, StorePassSteps.StoreMaintenancePass, "outside_window", now.AddHours(-30), 1);
            await SeedAsync(connection, ct, StorePassSteps.StoreSelfMetricsPass, "newest_inside", now.AddMinutes(-10), 2.5);
            await SeedAsync(connection, ct, StorePassSteps.QueryStoreComposePass, "middle_inside", now.AddHours(-3), 1, StorePassSteps.OutcomeFailed, detail: "it broke");

            var answer = await ReadAsync(postgres, ct);

            Assert.Equal(new[] { "newest_inside", "middle_inside", "oldest_inside" }, Steps(answer));
            Assert.Equal(3, answer.GetProperty("rows_total").GetInt64());
            Assert.Equal(3, answer.GetProperty("rows_returned").GetInt32());
            Assert.False(answer.GetProperty("budget_truncated").GetBoolean());

            var rows = answer.GetProperty("steps").EnumerateArray().ToArray();
            Assert.Equal(2.5, rows[0].GetProperty("duration_seconds").GetDouble(), 3);
            Assert.Equal(JsonValueKind.Null, rows[0].GetProperty("built").ValueKind);
            Assert.Equal(JsonValueKind.Null, rows[0].GetProperty("removed").ValueKind);
            Assert.Equal(JsonValueKind.Null, rows[0].GetProperty("detail").ValueKind);
            Assert.Equal(StorePassSteps.OutcomeFailed, rows[1].GetProperty("outcome").GetString());
            Assert.Equal("it broke", rows[1].GetProperty("detail").GetString());
            Assert.Equal(7, rows[2].GetProperty("built").GetInt64());
            Assert.Equal(2, rows[2].GetProperty("removed").GetInt64());
        });
    }

    [Fact]
    public async Task AsOf_MovesTheWindowEnd_SoALaterStepIsLeftOut()
    {
        await WithStoreAsync(async (connection, postgres, ct) =>
        {
            var now = DateTime.UtcNow;
            await SeedAsync(connection, ct, StorePassSteps.StoreMaintenancePass, "before_anchor", now.AddHours(-5), 1);
            await SeedAsync(connection, ct, StorePassSteps.StoreMaintenancePass, "after_anchor", now.AddHours(-1), 1);

            var anchor = now.AddHours(-3).ToString("o", System.Globalization.CultureInfo.InvariantCulture);
            var answer = await ReadAsync(postgres, ct, hours: 6, asOf: anchor);

            Assert.Equal(new[] { "before_anchor" }, Steps(answer));
        });
    }

    [Fact]
    public async Task PassAndStepFilters_NarrowTheRows_AndTheTotalFollowsThem()
    {
        await WithStoreAsync(async (connection, postgres, ct) =>
        {
            var now = DateTime.UtcNow;
            await SeedAsync(connection, ct, StorePassSteps.StoreMaintenancePass, "module_map_refresh", now.AddMinutes(-50), 1);
            await SeedAsync(connection, ct, StorePassSteps.StoreMaintenancePass, "module_map_refresh", now.AddMinutes(-110), 1);
            await SeedAsync(connection, ct, StorePassSteps.StoreMaintenancePass, "job_outcome_daily", now.AddMinutes(-40), 1);
            await SeedAsync(connection, ct, StorePassSteps.StoreSelfMetricsPass, "module_map_refresh", now.AddMinutes(-30), 1);
            await SeedAsync(connection, ct, StorePassSteps.StoreSelfMetricsPass, "store_log_capture", now.AddMinutes(-20), 1);

            var byPass = await ReadAsync(postgres, ct, pass: StorePassSteps.StoreMaintenancePass);
            Assert.Equal(new[] { "job_outcome_daily", "module_map_refresh", "module_map_refresh" }, Steps(byPass));
            Assert.Equal(3, byPass.GetProperty("rows_total").GetInt64());

            var byStep = await ReadAsync(postgres, ct, step: "module_map_refresh");
            Assert.Equal(3, byStep.GetProperty("rows_total").GetInt64());
            Assert.All(byStep.GetProperty("steps").EnumerateArray(), s => Assert.Equal("module_map_refresh", s.GetProperty("step").GetString()));

            var both = await ReadAsync(postgres, ct, pass: StorePassSteps.StoreSelfMetricsPass, step: "module_map_refresh");
            Assert.Equal(1, both.GetProperty("rows_total").GetInt64());
            Assert.Equal(StorePassSteps.StoreSelfMetricsPass, both.GetProperty("steps")[0].GetProperty("pass").GetString());
        });
    }

    [Fact]
    public async Task Limit_CutsThePageToTheNewestRows_AndTheTotalSaysHowManyMatched()
    {
        await WithStoreAsync(async (connection, postgres, ct) =>
        {
            var now = DateTime.UtcNow;
            for (var i = 0; i < 5; i++)
            {
                await SeedAsync(connection, ct, StorePassSteps.StoreMaintenancePass, "step_" + i, now.AddMinutes(-(10 * (i + 1))), 1);
            }

            var answer = await ReadAsync(postgres, ct, limit: 2);

            Assert.Equal(new[] { "step_0", "step_1" }, Steps(answer));
            Assert.Equal(5, answer.GetProperty("rows_total").GetInt64());
            Assert.Equal(2, answer.GetProperty("rows_returned").GetInt32());
        });
    }

    [Fact]
    public async Task AnEmptyWindow_AnswersEmpty_NotAnError()
    {
        await WithStoreAsync(async (connection, postgres, ct) =>
        {
            await SeedAsync(connection, ct, StorePassSteps.StoreMaintenancePass, "long_ago", DateTime.UtcNow.AddHours(-100), 1);

            var answer = await ReadAsync(postgres, ct);

            Assert.Equal("empty", answer.GetProperty("status").GetString());
        });
    }

    [Fact]
    public async Task AStoreWithoutTheTable_AnswersPrecondition_NotAnError()
    {
        await WithStoreAsync(async (connection, postgres, ct) =>
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, "DROP TABLE collect.store_pass_steps");

            var answer = await ReadAsync(postgres, ct);

            Assert.Equal("precondition", answer.GetProperty("status").GetString());
        });
    }

    [Fact]
    public async Task TheWebMirror_ReadsTheSameRows()
    {
        await WithStoreAsync(async (connection, postgres, ct) =>
        {
            await SeedAsync(connection, ct, StorePassSteps.StoreMaintenancePass, "mirror_step", DateTime.UtcNow.AddMinutes(-5), 1);

            var json = await DarlingWebEndpoints.BuildReadDispatch()["get_store_pass_steps"](new DefaultHttpContext(), postgres, null!);
            var answer = JsonDocument.Parse(json).RootElement;

            Assert.Equal(new[] { "mirror_step" }, Steps(answer));
        });
    }
}
