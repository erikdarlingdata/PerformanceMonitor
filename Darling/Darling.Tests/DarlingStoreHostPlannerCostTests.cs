/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9179: <c>get_store_host</c> reports the planner cost settings (<c>random_page_cost</c>, <c>seq_page_cost</c>,
/// <c>effective_io_concurrency</c>) with their source and whether each matches the managed value, and says so in one
/// sentence when <c>random_page_cost</c> is not the managed value. The managed file writes <c>random_page_cost = 1.1</c>
/// and no other planner cost, so the other two report no managed value.
/// </summary>
/* #1776 own-store: the live tests mint their own scratch database through ScratchPostgres. */
public sealed class DarlingStoreHostPlannerCostTests
{
    private static readonly string DataDir = Path.Combine(Path.GetTempPath(), "darling-planner-cost-data");

    [Fact]
    public void ShapePlannerCost_AutoConfValue_ReportsFileAndMismatch_AndSentenceNamesSourceAndFile()
    {
        var row = DarlingStoreHostProfile.ShapePlannerCost(
            "random_page_cost", "4", "configuration file", Path.Combine(DataDir, "postgresql.auto.conf"), DataDir);

        Assert.Equal("configuration file", row.Source);
        Assert.Equal("postgresql.auto.conf", row.SourceFile);
        Assert.Equal("1.1", row.ManagedValue);
        Assert.False(row.MatchesManaged);

        var sentence = DarlingStoreHostProfile.DescribeRandomPageCostMismatch([row]);
        Assert.NotNull(sentence);
        Assert.Contains("configuration file", sentence);
        Assert.Contains("postgresql.auto.conf", sentence);
        Assert.Contains("4", sentence);
        Assert.DoesNotContain(DataDir, sentence);
    }

    [Fact]
    public void ShapePlannerCost_NullSourceFile_ReportsTheSourceAlone_NoGuessedFile()
    {
        var row = DarlingStoreHostProfile.ShapePlannerCost("random_page_cost", "4", "configuration file", null, DataDir);

        Assert.Null(row.SourceFile);
        Assert.False(row.MatchesManaged);

        var sentence = DarlingStoreHostProfile.DescribeRandomPageCostMismatch([row]);
        Assert.NotNull(sentence);
        Assert.Contains("configuration file", sentence);
        Assert.DoesNotContain("postgresql", sentence);
        Assert.DoesNotContain(".conf", sentence);
    }

    [Theory]
    [InlineData("1.1")]
    [InlineData("1.10")]
    public void ShapePlannerCost_ManagedValueInForce_MatchesAndSaysNothing(string live)
    {
        var row = DarlingStoreHostProfile.ShapePlannerCost(
            "random_page_cost", live, "configuration file", Path.Combine(DataDir, "darling-managed.conf"), DataDir);

        Assert.True(row.MatchesManaged);
        Assert.Null(DarlingStoreHostProfile.DescribeRandomPageCostMismatch([row]));
    }

    [Fact]
    public void ShapePlannerCost_StoreNotOnTheManagedRuntime_LeavesManagedValueAndMatchNull_AndSaysNothing()
    {
        var row = DarlingStoreHostProfile.ShapePlannerCost("random_page_cost", "4", "default", null, dataDirectory: null);

        Assert.Equal("4", row.Current);
        Assert.Equal("default", row.Source);
        Assert.Null(row.ManagedValue);
        Assert.Null(row.MatchesManaged);
        Assert.Null(DarlingStoreHostProfile.DescribeRandomPageCostMismatch([row]));
    }

    [Theory]
    [InlineData("seq_page_cost", "1")]
    [InlineData("effective_io_concurrency", "16")]
    public void ShapePlannerCost_SettingTheManagedFileDoesNotWrite_HasNoManagedValue(string name, string live)
    {
        var row = DarlingStoreHostProfile.ShapePlannerCost(name, live, "default", null, DataDir);

        Assert.Null(row.ManagedValue);
        Assert.Null(row.MatchesManaged);
        Assert.Null(DarlingStoreHostProfile.DescribeRandomPageCostMismatch([row]));
    }

    [Fact]
    public async Task GetStoreHost_DatabaseLevelRandomPageCost4_ReportsValueSourceAndMismatch_Live()
    {
        var json = await ReadStoreHostWithDatabaseRandomPageCostAsync("4");

        var costs = json.GetProperty("planner_costs").EnumerateArray().ToDictionary(c => c.GetProperty("name").GetString()!);
        Assert.Equal(new[] { "random_page_cost", "seq_page_cost", "effective_io_concurrency" }.Order(), costs.Keys.Order());

        var random = costs["random_page_cost"];
        Assert.Equal("4", random.GetProperty("current").GetString());
        Assert.Equal("database", random.GetProperty("source").GetString());
        Assert.Equal("1.1", random.GetProperty("managed_value").GetString());
        Assert.False(random.GetProperty("matches_managed").GetBoolean());

        var warning = json.GetProperty("planner_cost_warning").GetString();
        Assert.NotNull(warning);
        Assert.Contains("database", warning);

        Assert.Equal(JsonValueKind.Null, costs["seq_page_cost"].GetProperty("matches_managed").ValueKind);
        Assert.Equal(JsonValueKind.Null, costs["effective_io_concurrency"].GetProperty("managed_value").ValueKind);
    }

    [Fact]
    public async Task GetStoreHost_DatabaseLevelRandomPageCostAtTheManagedValue_ReportsMatching_Live()
    {
        var json = await ReadStoreHostWithDatabaseRandomPageCostAsync("1.1");

        var random = json.GetProperty("planner_costs").EnumerateArray().Single(c => c.GetProperty("name").GetString() == "random_page_cost");
        Assert.Equal("1.1", random.GetProperty("current").GetString());
        Assert.True(random.GetProperty("matches_managed").GetBoolean());
        Assert.Equal(JsonValueKind.Null, json.GetProperty("planner_cost_warning").ValueKind);
    }

    [Fact]
    public async Task WebEndpoint_GetStoreHost_ReturnsThePlannerCostFields_Live()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the get_store_host web endpoint test.");

        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var raw = await DarlingWebEndpoints.BuildReadDispatch(null, new PostgresConfig { Managed = false })["get_store_host"](
            new DefaultHttpContext { RequestAborted = ct }, postgres, null!);
        using var doc = JsonDocument.Parse(raw);

        var costs = doc.RootElement.GetProperty("planner_costs").EnumerateArray().ToList();
        Assert.Contains(costs, c => c.GetProperty("name").GetString() == "random_page_cost");
        Assert.All(costs, c => Assert.Equal(JsonValueKind.Null, c.GetProperty("matches_managed").ValueKind));
        Assert.True(doc.RootElement.TryGetProperty("planner_cost_warning", out _));
    }

    /* #1776 own-store: the value is set with ALTER DATABASE on a scratch database of this test's own. Never ALTER SYSTEM,
       because the live tests share one server and a cluster-wide random_page_cost would change other tests' plans. */
    private static async Task<JsonElement> ReadStoreHostWithDatabaseRandomPageCostAsync(string value)
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the get_store_host planner cost live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct, emptyStore: true);
        var bodySucceeded = false;
        var dataDirectory = Directory.CreateTempSubdirectory("darling-planner-cost-");
        try
        {
            await using (var admin = new NpgsqlConnection(cs))
            {
                await admin.OpenAsync(ct);
                await using var alter = new NpgsqlCommand(
                    $"ALTER DATABASE \"{scratch.DatabaseName}\" SET random_page_cost = {value}", admin);
                await alter.ExecuteNonQueryAsync(ct);
            }

            /* A data source opened after the ALTER, so no pooled session predates it. */
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var raw = await DarlingMcpStoreHostTools.GetStoreHost(
                postgres, new PostgresConfig { Managed = true, DataDirectory = dataDirectory.FullName },
                new StoreHostProfileCache(TimeSpan.FromMinutes(5)), cancellationToken: ct);

            var json = JsonDocument.Parse(raw).RootElement.Clone();
            bodySucceeded = true;
            return json;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
            try { dataDirectory.Delete(recursive: true); } catch (IOException) { }
        }
    }
}
