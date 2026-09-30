/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using ModelContextProtocol.Server;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite never captures execution plans: <c>CollectorContext.CapturePlanXml</c> defaults to false and Lite never
/// sets it, so <c>query_stats.query_plan_xml</c> is NULL on every row it collects. The three plan tools that read
/// that column (<c>analyze_query_plan</c>, <c>analyze_procedure_plan</c>, <c>get_plan_xml</c>) used to answer
/// <c>unavailable</c> with "the query may have been evicted from the plan cache", which reads as a true negative
/// about the monitored server when the truth is that Lite does not keep plans at all. They now answer
/// <c>not_collected</c> with one message that says so and names the way out (<c>analyze_plan_xml</c>, or Darling,
/// which keeps plans).
///
/// <para>Each pin seeds a server that HAS query_stats rows for the asked-about hash and no plan text, because that
/// is the state every Lite server is in: a pin over an empty store would pass for the wrong reason (an unknown
/// hash and a hash Lite merely never kept a plan for look identical there). Lite derives its server id from the
/// storage name, so the seeded rows are written under the same derived value the tool resolves to.</para>
/// </summary>
public sealed class PlanToolsNotKeptTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string BoxServerName = "LitePlanToolsBox";
    private const string AzureServerName = "LitePlanToolsAzure";
    private const string QueryHashNoPlan = "0xA1A1A1A1A1A1A1A1";
    private const string PlanHandleNoPlan = "0x06000500A1A1A1A1A1A1A1A1";
    private const string QueryHashWithPlan = "0xB2B2B2B2B2B2B2B2";
    private const string PlanHandleWithPlan = "0x06000500B2B2B2B2B2B2B2B2";

    /// <summary>The smallest showplan document the analyzer parses into one statement.</summary>
    private const string TrivialPlanXml =
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.5\" Build=\"16.0.1000.6\">" +
        "<BatchSequence><Batch><Statements>" +
        "<StmtSimple StatementText=\"SELECT 1\" StatementId=\"1\" StatementCompId=\"1\" StatementType=\"SELECT\" " +
        "StatementSubTreeCost=\"0.0000012\" StatementEstRows=\"1\" StatementOptmLevel=\"TRIVIAL\" CardinalityEstimationModelVersion=\"160\">" +
        "<QueryPlan CachedPlanSize=\"16\" CompileTime=\"0\" CompileCPU=\"0\" CompileMemory=\"96\">" +
        "<RelOp NodeId=\"0\" PhysicalOp=\"Constant Scan\" LogicalOp=\"Constant Scan\" EstimateRows=\"1\" EstimateIO=\"0\" " +
        "EstimateCPU=\"1.157e-06\" AvgRowSize=\"9\" EstimatedTotalSubtreeCost=\"1.157e-06\" Parallel=\"0\" " +
        "EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">" +
        "<OutputList /><ConstantScan />" +
        "</RelOp></QueryPlan></StmtSimple>" +
        "</Statements></Batch></BatchSequence></ShowPlanXML>";

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly int _boxServerId;
    private readonly int _azureServerId;
    private DuckDBConnection? _seedConn;
    private long _nextCollectionId = 1;

    public PlanToolsNotKeptTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-plantools-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);

        _boxServerId = Register(BoxServerName);
        _azureServerId = Register(AzureServerName);
    }

    private int Register(string name)
    {
        var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = name, IsEnabled = true };
        _serverManager.AddServer(server);
        return RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    [Fact]
    public async Task AnalyzeQueryPlan_WithQueryStatsRowsAndNoPlanText_SaysLiteKeepsNoPlans()
    {
        await SeedBoxWithQueryStatsRowsAsync();
        var service = new LocalDataService(_duckDb);

        var answer = await McpPlanTools.AnalyzeQueryPlan(service, _serverManager, QueryHashNoPlan, BoxServerName);

        AssertPlansNotKept(answer);
    }

    [Fact]
    public async Task AnalyzeProcedurePlan_WithQueryStatsRowsAndNoPlanText_SaysLiteKeepsNoPlans()
    {
        await SeedBoxWithQueryStatsRowsAsync();
        var service = new LocalDataService(_duckDb);

        var answer = await McpPlanTools.AnalyzeProcedurePlan(service, _serverManager, PlanHandleNoPlan, BoxServerName);

        AssertPlansNotKept(answer);
    }

    [Fact]
    public async Task GetPlanXml_WithQueryStatsRowsAndNoPlanText_SaysLiteKeepsNoPlans()
    {
        await SeedBoxWithQueryStatsRowsAsync();
        var service = new LocalDataService(_duckDb);

        var answer = await McpPlanTools.GetPlanXml(service, _serverManager, QueryHashNoPlan, BoxServerName);

        AssertPlansNotKept(answer);
    }

    /// <summary>
    /// The read still comes first. A row that DOES carry plan text (a store that kept plans under an older build)
    /// is served exactly as before, so the new answer is the miss answer, not a blanket refusal of the tool.
    /// </summary>
    [Fact]
    public async Task APlanThatWasStored_IsStillAnalyzedAndReturned()
    {
        await SeedBoxWithQueryStatsRowsAsync();
        var service = new LocalDataService(_duckDb);

        var analysis = JsonDocument.Parse(await McpPlanTools.AnalyzeQueryPlan(service, _serverManager, QueryHashWithPlan, BoxServerName)).RootElement;
        Assert.False(analysis.TryGetProperty("status", out _));
        Assert.Equal("query_stats", analysis.GetProperty("source").GetString());
        Assert.Equal(1, analysis.GetProperty("statement_count").GetInt32());

        var procedure = JsonDocument.Parse(await McpPlanTools.AnalyzeProcedurePlan(service, _serverManager, PlanHandleWithPlan, BoxServerName)).RootElement;
        Assert.False(procedure.TryGetProperty("status", out _));
        Assert.Equal(1, procedure.GetProperty("statement_count").GetInt32());

        var raw = await McpPlanTools.GetPlanXml(service, _serverManager, QueryHashWithPlan, BoxServerName);
        Assert.Equal(TrivialPlanXml, raw);
    }

    /// <summary>
    /// The engine answer stays first. A collector the server's engine cannot run is a permanent gap, stated with
    /// the engine's own words; only an engine that CAN collect it falls through to "Lite keeps no plans". Both
    /// directions, because a pin that only checked the Azure arm would pass if the plans-not-kept answer had
    /// started winning everywhere. <c>default_trace_events</c> stands in for the collector name because it is
    /// gated off on Azure SQL Database today; the plan tools pass <c>query_stats</c> / <c>procedure_stats</c>
    /// to the same method.
    /// </summary>
    [Fact]
    public async Task AnEngineThatCannotCollectTheReadKeepsItsOwnAnswerFirst()
    {
        await SeedServerRowAsync(_azureServerId, AzureServerName, CollectorEngineCapability.AzureSqlDatabaseEngineEdition);
        await SeedServerRowAsync(_boxServerId, BoxServerName, engineEdition: 3);
        var service = new LocalDataService(_duckDb);

        var azure = await McpPlanTools.PlansNotKeptAsync(service, _azureServerId, AzureServerName, "default_trace_events");
        Assert.Equal("not_collected", StatusOf(azure));
        Assert.Contains("Azure SQL Database", azure, StringComparison.Ordinal);
        Assert.DoesNotContain(McpPlanTools.PlansNotKeptMessage, azure, StringComparison.Ordinal);

        var box = await McpPlanTools.PlansNotKeptAsync(service, _boxServerId, BoxServerName, "default_trace_events");
        AssertPlansNotKept(box);
    }

    /// <summary>
    /// <c>analyze_plan_xml</c> is the way out the message names, so it has to keep working on plan XML passed to
    /// it, and it must not start asking the store for anything.
    /// </summary>
    [Fact]
    public void AnalyzePlanXml_StillAnalyzesPlanXmlPassedToIt()
    {
        var analysis = JsonDocument.Parse(McpPlanTools.AnalyzePlanXml(TrivialPlanXml)).RootElement;

        Assert.False(analysis.TryGetProperty("status", out _));
        Assert.Equal("xml", analysis.GetProperty("source").GetString());
        Assert.Equal(1, analysis.GetProperty("statement_count").GetInt32());
    }

    /// <summary>
    /// A model picking tools from tools/list sees only the description, so the three changed tools say there that
    /// Lite does not keep plans and name <c>analyze_plan_xml</c>. <c>analyze_query_store_plan</c> is not among them:
    /// it reads no stored plan column (it fetches from Query Store on the monitored instance), so what it says
    /// about a missing plan is still true.
    /// </summary>
    [Theory]
    [InlineData("analyze_query_plan")]
    [InlineData("analyze_procedure_plan")]
    [InlineData("get_plan_xml")]
    public void TheChangedToolsSayInTheirDescriptionThatLiteKeepsNoPlans(string tool)
    {
        var head = PerformanceMonitor.Common.McpToolGuide.Split(DescriptionOf(tool)).Head;

        Assert.Contains("Lite does not keep plans", head, StringComparison.Ordinal);
        Assert.Contains("analyze_plan_xml", head, StringComparison.Ordinal);
        Assert.Contains("not_collected", head, StringComparison.Ordinal);
    }

    /// <summary>The two tools that carry a <c>get_tool_guide</c> tail explain the cause there, where the space is
    /// free: the head says what to do, the tail says why an eviction is the wrong guess.</summary>
    [Theory]
    [InlineData("analyze_query_plan")]
    [InlineData("analyze_procedure_plan")]
    public void TheGuideTailExplainsThatLiteNeverCapturesPlans(string tool)
    {
        var tail = PerformanceMonitor.Common.McpToolGuide.Split(DescriptionOf(tool)).Tail;

        Assert.NotNull(tail);
        Assert.Contains("Lite never captures plans", tail, StringComparison.Ordinal);
        Assert.Contains("never a sign the plan was evicted", tail, StringComparison.Ordinal);
        Assert.Contains("analyze_plan_xml", tail, StringComparison.Ordinal);
    }

    [Fact]
    public void TheQueryStorePlanToolKeepsItsOwnDescription()
    {
        var whole = DescriptionOf("analyze_query_store_plan");

        Assert.DoesNotContain("does not keep", whole, StringComparison.Ordinal);
        Assert.Contains("Fetches the plan on-demand from the monitored SQL Server instance.", whole, StringComparison.Ordinal);
    }

    [Fact]
    public void TheMessage_SaysLiteKeepsNoPlans_AndNamesTheWayOut()
    {
        Assert.StartsWith("Lite does not keep query plans.", McpPlanTools.PlansNotKeptMessage, StringComparison.Ordinal);
        Assert.Contains("analyze_plan_xml", McpPlanTools.PlansNotKeptMessage, StringComparison.Ordinal);
        Assert.Contains("Darling", McpPlanTools.PlansNotKeptMessage, StringComparison.Ordinal);
        Assert.DoesNotContain("evicted", McpPlanTools.PlansNotKeptMessage, StringComparison.Ordinal);
    }

    private static void AssertPlansNotKept(string answer)
    {
        var root = JsonDocument.Parse(answer).RootElement;

        Assert.Equal("not_collected", root.GetProperty("status").GetString());
        Assert.Equal(McpPlanTools.PlansNotKeptMessage, root.GetProperty("message").GetString());
        Assert.DoesNotContain("evicted", answer, StringComparison.Ordinal);
    }

    private static string StatusOf(string json) =>
        JsonDocument.Parse(json).RootElement.GetProperty("status").GetString()!;

    private static string DescriptionOf(string toolName) => typeof(McpPlanTools)
        .GetMethods(BindingFlags.Public | BindingFlags.Static)
        .Single(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name == toolName)
        .GetCustomAttribute<System.ComponentModel.DescriptionAttribute>()!.Description;

    /// <summary>The box server (engine edition 3, probed) with query_stats rows for two hashes: one with no plan
    /// text (NULL on one row, empty on another, the two shapes the read filters out) and one whose row kept a
    /// plan.</summary>
    private async Task SeedBoxWithQueryStatsRowsAsync()
    {
        await SeedServerRowAsync(_boxServerId, BoxServerName, engineEdition: 3);

        var now = DateTime.UtcNow;
        await SeedQueryStatsRowAsync(QueryHashNoPlan, PlanHandleNoPlan, now.AddMinutes(-10), planXml: null);
        await SeedQueryStatsRowAsync(QueryHashNoPlan, PlanHandleNoPlan, now.AddMinutes(-5), planXml: "");
        await SeedQueryStatsRowAsync(QueryHashWithPlan, PlanHandleWithPlan, now.AddMinutes(-5), planXml: TrivialPlanXml);
    }

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        return _seedConn;
    }

    /// <summary>Column list copied from <c>EngineCapabilityMissTests</c>' servers seed: the one column these
    /// tests vary is the probed engine edition.</summary>
    private async Task SeedServerRowAsync(int serverId, string serverName, int engineEdition)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO servers (server_id, server_name, display_name, use_windows_auth, is_enabled, sql_engine_edition)
VALUES ($1, $2, $3, true, true, $4)";
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverName });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverName });
        cmd.Parameters.Add(new DuckDBParameter { Value = engineEdition });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedQueryStatsRowAsync(string queryHash, string planHandle, DateTime collectionTime, string? planXml)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, plan_handle,
     delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, query_text, query_plan_xml)
VALUES ($1, $2, $3, $4, 'PlanToolsDb', $5, $6, 10, 500000, 900000, 1000, 'SELECT 1', $7)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextCollectionId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = _boxServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = BoxServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = planHandle });
        cmd.Parameters.Add(new DuckDBParameter { Value = planXml is null ? DBNull.Value : planXml });
        await cmd.ExecuteNonQueryAsync();
    }
}
