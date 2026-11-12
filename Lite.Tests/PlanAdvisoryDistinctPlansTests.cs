/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7886: the plan advisory facts and their drill-down count each plan once, however many snapshots it has
/// in the window. <c>v_query_stats</c> holds one row per query per snapshot, and every snapshot row of a cached
/// plan carries the same plan XML, so a hot plan seen in six collections is six rows at the top of the
/// delta_worker_time ordering. PLAN_WARNING's <c>warning_count</c> and <c>critical_count</c> are the warnings
/// over the window's distinct top plans, and the drill-down's <c>plan_warnings</c> never lists one plan's
/// warning twice. The Darling twin is <c>PlanAdvisoryDistinctPlansLiveTests</c>.
///
/// <para>A plan is a statement's plan, not a plan shape: <c>query_plan_hash</c> is a shape hash that two
/// statements with one shape share, so two such statements with different plans are two plans and both are
/// counted.</para>
///
/// <para>Real DuckDB (the shared fixture) because the read is the SQL itself: the fact collector and the
/// drill-down both read the plans through <c>v_query_stats</c>.</para>
/// </summary>
public sealed class PlanAdvisoryDistinctPlansTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 788_601;
    private const string ServerName = "PlanDedupLiteSrv";

    private static readonly DateTime WindowEnd = DateTime.SpecifyKind(
        new DateTime(DateTime.UtcNow.Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond), DateTimeKind.Unspecified);

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public PlanAdvisoryDistinctPlansTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    /// <summary>A plan whose one statement wraps a column in a function, so the analyzer raises a warning that quotes the predicate.</summary>
    private static string WarningPlan(string column) =>
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.564\" Build=\"16.0.4215.2\">"
        + "<BatchSequence><Batch><Statements>"
        + "<StmtSimple StatementText=\"" + WebUtility.HtmlEncode($"SELECT * FROM dbo.t WHERE UPPER({column}) = 1") + "\" StatementId=\"1\" StatementCompId=\"1\" StatementType=\"SELECT\" "
        + "StatementSubTreeCost=\"5\" StatementOptmLevel=\"FULL\"><QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"104\">"
        + "<RelOp NodeId=\"0\" PhysicalOp=\"Table Scan\" LogicalOp=\"Table Scan\" EstimateRows=\"100\" EstimateIO=\"1\" EstimateCPU=\"0.1\" AvgRowSize=\"9\" "
        + "EstimatedTotalSubtreeCost=\"5\" TableCardinality=\"100\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">"
        + "<OutputList/><TableScan Ordered=\"0\" ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\"><DefinedValues/>"
        + "<Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" IndexKind=\"Heap\" Storage=\"RowStore\"/>"
        + "<Predicate><ScalarOperator ScalarString=\"" + WebUtility.HtmlEncode($"upper([db].[dbo].[t].[{column}])=(1)") + "\"><Const ConstValue=\"(1)\"/></ScalarOperator></Predicate>"
        + "</TableScan></RelOp></QueryPlan></StmtSimple>"
        + "</Statements></Batch></BatchSequence></ShowPlanXML>";

    private static readonly string HotPlan = WarningPlan("c_hot");
    private static readonly string SecondPlan = WarningPlan("c_second");
    private static readonly string ThirdPlan = WarningPlan("c_third");
    private static readonly string FirstStatementPlan = WarningPlan("c_first_statement");
    private static readonly string SecondStatementPlan = WarningPlan("c_second_statement");

    private static AnalysisContext Context() => new()
    {
        ServerId = ServerId,
        ServerName = ServerName,
        TimeRangeStart = WindowEnd.AddHours(-4),
        TimeRangeEnd = WindowEnd,
    };

    /// <summary>
    /// The hot plan in six snapshots, each costlier than the two single-snapshot plans, so the six copies lead
    /// the delta_worker_time ordering the read takes its ten rows from.
    /// </summary>
    private async Task SeedHotPlanInSixSnapshotsPlusTwoOthersAsync()
    {
        for (var i = 0; i < 6; i++)
            await SeedAsync(WindowEnd.AddMinutes(-5 * (i + 1)), "0xPLANHOT", HotPlan, workerTime: 900_000 - i);
        await SeedAsync(WindowEnd.AddMinutes(-10), "0xPLANSECOND", SecondPlan, workerTime: 400_000);
        await SeedAsync(WindowEnd.AddMinutes(-10), "0xPLANTHIRD", ThirdPlan, workerTime: 300_000);
    }

    /// <summary>
    /// Two statements of one batch with one plan shape: the same query_plan_hash and plan_handle, a different
    /// query_hash, and a different plan each, in one snapshot. Both plans raise a warning of their own.
    /// </summary>
    private async Task SeedTwoStatementsWithOnePlanShapeAsync()
    {
        await SeedAsync(WindowEnd.AddMinutes(-5), "0xSHAPE", FirstStatementPlan, workerTime: 900_000, queryHash: "0xSTMT1", planHandle: "0xBATCH");
        await SeedAsync(WindowEnd.AddMinutes(-5), "0xSHAPE", SecondStatementPlan, workerTime: 800_000, queryHash: "0xSTMT2", planHandle: "0xBATCH");
    }

    [Fact]
    public async Task PlanWarningFact_TwoStatementsWithOnePlanShape_CountsBothPlans()
    {
        await SeedTwoStatementsWithOnePlanShapeAsync();
        var both = PlanAdvisoryAggregator.Summarize([FirstStatementPlan, SecondStatementPlan]);
        Assert.True(both.WarningCount > PlanAdvisoryAggregator.Summarize([FirstStatementPlan]).WarningCount,
            "the second statement's plan must add a warning of its own");

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(Context());

        var fact = Assert.Single(facts, f => f.Key == "PLAN_WARNING");
        Assert.Equal(both.WarningCount, fact.Value);
        Assert.Equal(both.WarningCount, fact.Metadata["warning_count"]);
        Assert.Equal(both.CriticalCount, fact.Metadata["critical_count"]);
    }

    [Fact]
    public async Task PlanWarningDrillDown_TwoStatementsWithOnePlanShape_ListsBothPlansWarnings()
    {
        await SeedTwoStatementsWithOnePlanShapeAsync();
        var finding = new AnalysisFinding
        {
            RootFactKey = "PLAN_WARNING",
            StoryPath = "PLAN_WARNING",
            PathKeys = ["PLAN_WARNING"],
            Severity = 1.0,
        };

        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], Context());

        Assert.NotNull(finding.DrillDown);
        Assert.True(finding.DrillDown.TryGetValue("plan_warnings", out var raw), "plan_warnings was not collected");
        var warnings = JsonSerializer.Deserialize<List<DrillDownWarning>>(JsonSerializer.Serialize(raw))!;
        var both = PlanAdvisoryAggregator.Extract([FirstStatementPlan, SecondStatementPlan]).Warnings;
        Assert.True(both.Count > PlanAdvisoryAggregator.Extract([FirstStatementPlan]).Warnings.Count,
            "the second statement's plan must add a warning of its own");
        Assert.Equal(Math.Min(5, both.Count), warnings.Count);
        Assert.Equal(warnings.Count, warnings.Select(w => (w.type, w.message)).Distinct().Count());
    }

    [Fact]
    public async Task PlanWarningFact_APlanInSixSnapshots_CountsItsWarningsOnce()
    {
        await SeedHotPlanInSixSnapshotsPlusTwoOthersAsync();
        var distinct = PlanAdvisoryAggregator.Summarize([HotPlan, SecondPlan, ThirdPlan]);
        Assert.True(PlanAdvisoryAggregator.Summarize([HotPlan]).WarningCount >= 1, "the fixture plan must raise at least one warning");

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(Context());

        var fact = Assert.Single(facts, f => f.Key == "PLAN_WARNING");
        Assert.Equal(distinct.WarningCount, fact.Value);
        Assert.Equal(distinct.WarningCount, fact.Metadata["warning_count"]);
        Assert.Equal(distinct.CriticalCount, fact.Metadata["critical_count"]);
    }

    [Fact]
    public async Task PlanWarningDrillDown_APlanInSixSnapshots_ListsNoWarningTwice()
    {
        await SeedHotPlanInSixSnapshotsPlusTwoOthersAsync();
        var finding = new AnalysisFinding
        {
            RootFactKey = "PLAN_WARNING",
            StoryPath = "PLAN_WARNING",
            PathKeys = ["PLAN_WARNING"],
            Severity = 1.0,
        };

        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], Context());

        Assert.NotNull(finding.DrillDown);
        Assert.True(finding.DrillDown.TryGetValue("plan_warnings", out var raw), "plan_warnings was not collected");
        var warnings = JsonSerializer.Deserialize<List<DrillDownWarning>>(JsonSerializer.Serialize(raw))!;
        Assert.NotEmpty(warnings);
        var distinct = PlanAdvisoryAggregator.Extract([HotPlan, SecondPlan, ThirdPlan]).Warnings.Count;
        Assert.Equal(Math.Min(5, distinct), warnings.Count);
        Assert.Equal(warnings.Count, warnings.Select(w => (w.type, w.message)).Distinct().Count());
    }

    private sealed record DrillDownWarning(string type, string severity, string message);

    /// <summary>One query_stats snapshot row. The query hash and plan handle derive from the plan hash unless given.</summary>
    private async Task SeedAsync(DateTime collectionTime, string planHash, string planXml, long workerTime, string? queryHash = null, string? planHandle = null)
    {
        using var readLock = _duckDb.AcquireReadLock();
        _seedConn ??= _duckDb.CreateConnection();
        if (_seedConn.State != System.Data.ConnectionState.Open)
            await _seedConn.OpenAsync();

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, plan_handle,
     creation_time, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_spills,
     query_text, query_plan_xml)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 10, $10, 1000000, 1000, 0, $11, $12)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = "PlanDedupDb" });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryHash ?? planHash + "-query" });
        cmd.Parameters.Add(new DuckDBParameter { Value = planHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = planHandle ?? planHash + "-handle" });
        cmd.Parameters.Add(new DuckDBParameter { Value = WindowEnd.AddDays(-1) });
        cmd.Parameters.Add(new DuckDBParameter { Value = workerTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SELECT * FROM dbo.t" });
        cmd.Parameters.Add(new DuckDBParameter { Value = planXml });
        await cmd.ExecuteNonQueryAsync();
    }
}
