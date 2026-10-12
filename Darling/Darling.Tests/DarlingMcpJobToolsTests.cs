/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the SQL Agent running-jobs MCP slice — get_running_jobs over the Postgres store. Ungated: tool surface,
/// the single server_name param, the latest-snapshot read SQL pin (v_running_jobs), the duration formatting,
/// and the Gemini-clean advertised schema.
/// </summary>
public sealed class DarlingMcpJobToolsSurfaceAndSqlTests
{
    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpJobTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyTheJobTools()
    {
        var names = ToolMethods()
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .ToArray();

        Assert.Equal(new[] { "get_job_history", "get_running_jobs" }, names.OrderBy(n => n, StringComparer.Ordinal).ToArray());
        Assert.NotNull(typeof(DarlingMcpJobTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        Assert.All(ToolMethods(), m => Assert.True(m.IsStatic, $"{m.Name} must be static"));
        Assert.All(ToolMethods(), m => Assert.True(m.ReturnType == typeof(Task<string>), $"{m.Name} must return Task<string>"));
    }

    [Fact]
    public void ParamContract_ServerNameOnly_Optional()
    {
        var method = ToolMethods().Single(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name == "get_running_jobs");
        var p = method.GetParameters()
            .Where(x => x.GetCustomAttribute<DescriptionAttribute>() is not null)
            .Select(x => (x.Name!, x.HasDefaultValue))
            .ToArray();
        Assert.Equal(new[] { "server_name" }, p.Select(x => x.Item1).ToArray());
        Assert.True(p.Single().Item2);
    }

    [Fact]
    public void RunningJobsSql_LatestSnapshot_OrderedByDuration()
    {
        var sql = DarlingJobReader.RunningJobsSql;
        Assert.Contains("FROM v_running_jobs", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("current_duration_seconds", sql, StringComparison.Ordinal);
        Assert.Contains("avg_duration_seconds", sql, StringComparison.Ordinal);
        Assert.Contains("p95_duration_seconds", sql, StringComparison.Ordinal);
        Assert.Contains("is_running_long", sql, StringComparison.Ordinal);
        Assert.Contains("percent_of_average", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY current_duration_seconds DESC", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void RunningJobsSql_IsPostgresDialect_NoTsqlIsms()
    {
        var sql = DarlingJobReader.RunningJobsSql;
        var lower = sql.ToLowerInvariant();
        Assert.DoesNotContain("getdate", lower);
        Assert.DoesNotContain("convert(", lower);
        Assert.DoesNotContain("top (", lower);
        Assert.DoesNotContain("isnull(", lower);
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0L, "0s")]
    [InlineData(45L, "45s")]
    [InlineData(90L, "1m 30s")]
    [InlineData(3600L, "1h 0m")]
    [InlineData(3725L, "1h 2m")]
    public void FormatDuration_MatchesLiteDashboard(long seconds, string expected)
    {
        Assert.Equal(expected, DarlingJobReader.FormatDuration(seconds));
    }

    [Fact]
    public void ReadColumns_ExistInTheGeneratedCollectorTable()
    {
        var rj = PgSchemaGenerator.CreateTable(RunningJobsCollector.Instance);
        Assert.Equal("running_jobs", RunningJobsCollector.Instance.TargetTable);
        Assert.Contains("job_name", rj, StringComparison.Ordinal);
        Assert.Contains("current_duration_seconds", rj, StringComparison.Ordinal);
        Assert.Contains("avg_duration_seconds", rj, StringComparison.Ordinal);
        Assert.Contains("p95_duration_seconds", rj, StringComparison.Ordinal);
        Assert.Contains("is_running_long", rj, StringComparison.Ordinal);
        Assert.Contains("percent_of_average", rj, StringComparison.Ordinal);

        Assert.Contains("v_running_jobs", PgSchemaGenerator.AllPassthroughViews);
    }

    private static List<ModelContextProtocol.Protocol.Tool> BuildToolSchemas()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpJobTools>();
        using var provider = services.BuildServiceProvider();
        return provider.GetServices<McpServerTool>().Select(t => t.ProtocolTool).ToList();
    }

    [Fact]
    public void AdvertisedSchema_IsGeminiClean_NoRequiredParams()
    {
        var tools = BuildToolSchemas();
        Assert.Equal(2, tools.Count);
        var violations = tools.SelectMany(t => DarlingMcpSchemaAssert.Violations(t.Name, t.InputSchema)).ToList();
        Assert.True(violations.Count == 0, "Gemini-incompatible schema keywords leaked:\n" + string.Join("\n", violations));
        foreach (var t in tools)
            Assert.Empty(DarlingMcpSchemaAssert.RequiredOf(t.InputSchema));
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trip for get_running_jobs. Plants a running_jobs row (note: running_jobs
/// is the one collector table with NO collection_id), then asserts the tool returns its data-bearing envelope;
/// an empty store returns the "empty" miss.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpJobToolsLivePostgresTests
{
    private const string ServerName = "darling-mcp-jobs-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task JobTools_ReadPlantedRows_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live running-jobs-tools test.");

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
            var t = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-2);

            /* running_jobs has NO collection_id column (IncludesCollectionId = false). */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO running_jobs (collection_time, server_id, server_name, job_name, job_id, job_enabled, start_time, current_duration_seconds, avg_duration_seconds, p95_duration_seconds, successful_run_count, is_running_long, percent_of_average)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
                t, ServerId, ServerName, "Nightly ETL", "11111111-1111-1111-1111-111111111111", true, t.AddMinutes(-30), 1800L, 600L, 900L, 42L, true, 300.0m);

            var jobs = await DarlingMcpJobTools.GetRunningJobs(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(jobs, ServerName, "jobs");
            Assert.Contains("Nightly ETL", jobs, StringComparison.Ordinal);
            Assert.Contains("current_duration_formatted", jobs, StringComparison.Ordinal);

            await DeleteRowsAsync(connection, ct, keepServer: true);
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(await DarlingMcpJobTools.GetRunningJobs(postgres, ServerName)));

            bodySucceeded = true;
        }
        finally
        {
            /* Fresh connection + body-aware masking (#1794): this class failed twice on a reused local
               store with "Connection is not open" thrown from cleanup — the body's connection is the one
               thing the reported failure may have destroyed. */
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteRowsAsync(cleanup, cleanupCt);
            });
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, bool keepServer = false)
    {
        var sql = $"DELETE FROM running_jobs WHERE server_id = {ServerId};";
        if (!keepServer) sql += $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
