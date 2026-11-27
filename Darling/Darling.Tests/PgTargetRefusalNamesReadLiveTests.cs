/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test mints its own scratch database through ScratchPostgres and never touches another test's
   rows, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #8942: a SQL Server read called on a PostgreSQL target refuses, and the refusal names the <c>get_pg_*</c> read that answers the same
/// question where one exists. <c>get_query_fleet_summary</c> and <c>get_finops_inventory</c> answer the engine status, not the true-negative
/// <c>empty</c>, on a store with no SQL Server target, and keep <c>empty</c> on a mixed or SQL Server store.
/// </summary>
public sealed class PgTargetRefusalNamesReadLiveTests
{
    private const int PgId = -894201;
    private const int SqlId = -894202;
    private const string PgName = "pg-8942";
    private const string SqlName = "sql-8942";

    private static string Cs => Environment.GetEnvironmentVariable("DARLING_TEST_PG") ?? string.Empty;

    [Fact]
    public void EveryNamedReadIsARegisteredTool()
    {
        var registered = typeof(DarlingEngineCapability).Assembly.GetTypes()
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance))
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name)
            .Where(n => n is not null)
            .ToHashSet(StringComparer.Ordinal);

        Assert.NotEmpty(DarlingEngineCapability.PostgresReadFor);
        Assert.All(DarlingEngineCapability.PostgresReadFor.Values, v => Assert.Contains(v.Read, registered));
        Assert.Contains("get_pg_top_queries", registered);
    }

    private static async Task<ScratchPostgres> SeedAsync(bool pg, bool sql, CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(Cs, ct);
        await using var c = new NpgsqlConnection(scratch.ConnectionString);
        await c.OpenAsync(ct);
        await PgMigrations.MigrateAsync(c, ct);
        if (pg)
        {
            await DarlingMcpTestData.RegisterServerAsync(c, PgId, PgName, ct);
            await DarlingMcpTestData.ExecAsync(c, ct, "UPDATE servers SET engine_kind = $2 WHERE server_id = $1", PgId, MonitoredEngineKind.Postgres);
        }

        if (sql)
        {
            await DarlingMcpTestData.RegisterServerAsync(c, SqlId, SqlName, ct);
            await DarlingMcpTestData.ExecAsync(c, ct, "UPDATE servers SET engine_kind = $2 WHERE server_id = $1", SqlId, MonitoredEngineKind.SqlServer);
        }

        return scratch;
    }

    private static (string Status, string Message) Parse(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return (doc.RootElement.GetProperty("status").GetString()!, doc.RootElement.GetProperty("message").GetString()!);
    }

    private static async Task RunAsync(bool pg, bool sql, Func<NpgsqlDataSource, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live PostgreSQL-target refusal tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(pg, sql, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var succeeded = false;
        try
        {
            await body(postgres, ct);
            succeeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, succeeded, (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public Task EachGatedRead_OnAPostgresOnlyStore_NamesItsMatchingPostgresRead() => RunAsync(true, false, async (postgres, ct) =>
    {
        var wait = Parse(await DarlingMcpDataTools.GetWaitTrend(postgres, "CXPACKET", PgName, cancellationToken: ct));
        Assert.Equal("not_collected", wait.Status);
        Assert.Contains("get_pg_wait_trend", wait.Message, StringComparison.Ordinal);

        var query = Parse(await DarlingMcpTrendTools.GetQueryTrend(postgres, "0xABC", "db", PgName, cancellationToken: ct));
        Assert.Equal("not_collected", query.Status);
        Assert.Contains("get_pg_query_duration_trend", query.Message, StringComparison.Ordinal);

        var session = Parse(await DarlingMcpServerTrendTools.GetServerTrend(postgres, "session_stats", PgName, cancellationToken: ct));
        Assert.Equal("not_collected", session.Status);
        Assert.Contains("get_pg_session_states", session.Message, StringComparison.Ordinal);

        /* No registered PostgreSQL read honestly answers memory (utilization is gated on memory_stats, which get_memory_stats and get_memory_trend
           share, and get_pg_cpu_utilization is CPU on Amazon Aurora only), and none serves database sizes, so these refusals name none. */
        var utilization = Parse(await DarlingMcpFinOpsTools.GetFinOps(postgres, "utilization", PgName, cancellationToken: ct));
        Assert.Equal("not_collected", utilization.Status);
        Assert.DoesNotContain("get_pg_", utilization.Message, StringComparison.Ordinal);

        var sizes = Parse(await DarlingMcpFinOpsTools.GetFinOps(postgres, "database_sizes", PgName, cancellationToken: ct));
        Assert.Equal("not_collected", sizes.Status);
        Assert.DoesNotContain("get_pg_", sizes.Message, StringComparison.Ordinal);
    });

    [Fact]
    public Task FleetSummaryAndInventory_OnAPostgresOnlyStore_AnswerTheEngineStatus() => RunAsync(true, false, async (postgres, ct) =>
    {
        var fleet = Parse(await DarlingMcpQueryFleetSummaryTools.GetQueryFleetSummary(postgres, "-4611686018427387904", 24, cancellationToken: ct));
        Assert.Equal("not_collected", fleet.Status);
        Assert.Contains("SQL Server", fleet.Message, StringComparison.Ordinal);
        Assert.Contains("get_pg_top_queries", fleet.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("get_top_queries_by_cpu", fleet.Message, StringComparison.Ordinal);

        var inventory = Parse(await DarlingMcpFinOpsInventoryTools.GetFinOpsInventory(postgres, "server_inventory", 10, cancellationToken: ct));
        Assert.Equal("not_collected", inventory.Status);
        Assert.Contains("SQL Server", inventory.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("yet", inventory.Message, StringComparison.Ordinal);
    });

    [Fact]
    public Task FleetSummaryAndInventory_OnAMixedStore_KeepEmpty_AndTheFleetSummaryNamesThePostgresRead() => RunAsync(true, true, async (postgres, ct) =>
    {
        var fleet = Parse(await DarlingMcpQueryFleetSummaryTools.GetQueryFleetSummary(postgres, "0xNOWHERE", 24, cancellationToken: ct));
        Assert.Equal("empty", fleet.Status);
        Assert.Contains("get_pg_top_queries", fleet.Message, StringComparison.Ordinal);

        Assert.Equal("empty", Parse(await DarlingMcpFinOpsInventoryTools.GetFinOpsInventory(postgres, "server_inventory", 10, cancellationToken: ct)).Status);
    });

    [Fact]
    public Task FleetSummaryAndInventory_UnderAPostgresOnlyScopeOnAMixedStore_AnswerTheEngineStatus_AndNameNoServerOutsideTheScope() =>
        RunAsync(true, true, async (postgres, ct) =>
        {
            var fleet = Parse(await DarlingServerResolver.ReadWebScopeAsync([PgId], [],
                () => DarlingMcpQueryFleetSummaryTools.GetQueryFleetSummary(postgres, "0xNOWHERE", 24, cancellationToken: ct)));
            Assert.Equal("not_collected", fleet.Status);
            Assert.Contains(PgName, fleet.Message, StringComparison.Ordinal);
            Assert.DoesNotContain(SqlName, fleet.Message, StringComparison.Ordinal);
            Assert.Contains("get_pg_top_queries", fleet.Message, StringComparison.Ordinal);

            var inventory = Parse(await DarlingServerResolver.ReadWebScopeAsync([PgId], [],
                () => DarlingMcpFinOpsInventoryTools.GetFinOpsInventory(postgres, "server_inventory", 10, cancellationToken: ct)));
            Assert.Equal("not_collected", inventory.Status);
            Assert.Contains(PgName, inventory.Message, StringComparison.Ordinal);
            Assert.DoesNotContain(SqlName, inventory.Message, StringComparison.Ordinal);
        });

    [Fact]
    public Task FleetSummary_UnderASqlServerOnlyScopeOnAMixedStore_KeepsEmpty_WithoutAPostgresRead() => RunAsync(true, true, async (postgres, ct) =>
    {
        var fleet = Parse(await DarlingServerResolver.ReadWebScopeAsync([SqlId], [],
            () => DarlingMcpQueryFleetSummaryTools.GetQueryFleetSummary(postgres, "0xNOWHERE", 24, cancellationToken: ct)));
        Assert.Equal("empty", fleet.Status);
        Assert.DoesNotContain("get_pg_", fleet.Message, StringComparison.Ordinal);
        Assert.Contains("get_top_queries_by_cpu", fleet.Message, StringComparison.Ordinal);
    });

    [Fact]
    public Task FleetSummaryAndInventory_OnASqlServerStore_KeepEmpty_WithoutAPostgresRead() => RunAsync(false, true, async (postgres, ct) =>
    {
        var fleet = Parse(await DarlingMcpQueryFleetSummaryTools.GetQueryFleetSummary(postgres, "0xNOWHERE", 24, cancellationToken: ct));
        Assert.Equal("empty", fleet.Status);
        Assert.DoesNotContain("get_pg_", fleet.Message, StringComparison.Ordinal);
        Assert.Contains("get_top_queries_by_cpu", fleet.Message, StringComparison.Ordinal);

        Assert.Equal("empty", Parse(await DarlingMcpFinOpsInventoryTools.GetFinOpsInventory(postgres, "server_inventory", 10, cancellationToken: ct)).Status);
    });
}
