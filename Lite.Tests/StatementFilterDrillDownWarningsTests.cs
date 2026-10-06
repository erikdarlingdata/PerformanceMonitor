/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Net;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #4348, L8b: Lite's drill-down plan warnings quote up to 300 characters of a predicate from the plan they analyzed.
/// <see cref="DrillDownCollector"/> judges the plan BEFORE it is analyzed at both of its plan reads: the live fetch
/// (<c>CollectPlanAnalysis</c>, a BAD_ACTOR finding) and the stored-plan read (<c>CollectPlanAdvisoryDetail</c>, a
/// PLAN_WARNING finding). A predicate that belongs to a withheld statement must be withheld with it, while the warning
/// for a statement that is kept still quotes its predicate. The Darling twin is
/// <c>StatementFilterDrillDownWarningsLiveTests</c>.
///
/// <para>Real DuckDB (the shared fixture, one seeded <c>query_stats</c> row) because the stored-plan site reads the
/// plan through <c>v_query_stats</c>; the live site gets its plan from a fake <see cref="IPlanFetcher"/>.</para>
/// </summary>
public sealed class StatementFilterDrillDownWarningsTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 453_202;
    private const string ServerName = "SsfLiteDrillDownSrv";
    private const string Hash = "0xSSF0LITEDRILL";
    private const string PlanHandle = "0x05SSF0LITEHANDLE";

    private static readonly DateTime WindowEnd = DateTime.SpecifyKind(
        new DateTime(DateTime.UtcNow.Ticks - (DateTime.UtcNow.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;

    public StatementFilterDrillDownWarningsTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    private static string Statement(string text, string predicate, string constant) =>
        "<StmtSimple StatementText=\"" + WebUtility.HtmlEncode(text) + "\" StatementId=\"1\" StatementCompId=\"1\" StatementType=\"SELECT\" "
        + "StatementSubTreeCost=\"5\" StatementOptmLevel=\"FULL\"><QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"104\">"
        + "<RelOp NodeId=\"0\" PhysicalOp=\"Table Scan\" LogicalOp=\"Table Scan\" EstimateRows=\"100\" EstimateIO=\"1\" EstimateCPU=\"0.1\" AvgRowSize=\"9\" "
        + "EstimatedTotalSubtreeCost=\"5\" TableCardinality=\"100\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">"
        + "<OutputList/><TableScan Ordered=\"0\" ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\"><DefinedValues/>"
        + "<Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" IndexKind=\"Heap\" Storage=\"RowStore\"/>"
        + "<Predicate><ScalarOperator ScalarString=\"" + WebUtility.HtmlEncode(predicate) + "\"><Const ConstValue=\"" + constant + "\"/></ScalarOperator></Predicate>"
        + "</TableScan></RelOp></QueryPlan></StmtSimple>";

    private static string TwoStatementPlan() =>
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.564\" Build=\"16.0.4215.2\">"
        + "<BatchSequence><Batch><Statements>"
        + Statement(StatementScrubCanary.CanaryStatement, "upper([db].[dbo].[t].[c])=N'const-secret-ssf'", "N'const-secret-ssf'")
        + Statement(StatementScrubCanary.PlainStatement, "upper([db].[dbo].[t].[c])=N'kept-predicate-ssf'", "N'kept-predicate-ssf'")
        + "</Statements></Batch></BatchSequence></ShowPlanXML>";

    private sealed class FakePlanFetcher(string planXml) : IPlanFetcher
    {
        public Task<string?> FetchPlanXmlAsync(int serverId, string planHandle, CancellationToken cancellationToken) =>
            Task.FromResult<string?>(planXml);
    }

    private static AnalysisContext Context() => new()
    {
        ServerId = ServerId,
        ServerName = ServerName,
        TimeRangeStart = WindowEnd.AddHours(-4),
        TimeRangeEnd = WindowEnd,
    };

    [Fact]
    public async Task LiveFetch_APredicateQuotedInAPlanWarning_IsWithheldWithItsStatement_AndKeptWithAKeptOne()
    {
        await SeedAsync(planXml: null);
        var finding = Finding("BAD_ACTOR_" + Hash);

        await new DrillDownCollector(_duckDb, new FakePlanFetcher(TwoStatementPlan())).EnrichFindingsAsync([finding], Context());

        AssertWarningsFiltered(finding, "plan_analysis");
    }

    [Fact]
    public async Task StoredPlanRead_APredicateQuotedInAPlanWarning_IsWithheldWithItsStatement_AndKeptWithAKeptOne()
    {
        await SeedAsync(planXml: TwoStatementPlan());
        var finding = Finding("PLAN_WARNING");

        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], Context());

        AssertWarningsFiltered(finding, "plan_warnings");
    }

    private static AnalysisFinding Finding(string factKey) => new()
    {
        RootFactKey = factKey,
        StoryPath = factKey,
        PathKeys = [factKey],
        Severity = 1.0,
    };

    private static void AssertWarningsFiltered(AnalysisFinding finding, string section)
    {
        Assert.NotNull(finding.DrillDown);
        Assert.True(finding.DrillDown.TryGetValue(section, out var raw), $"{section} was not collected");
        var text = JsonSerializer.Serialize(raw);
        Assert.Contains("kept-predicate-ssf", text, StringComparison.Ordinal);
        foreach (var needle in StatementScrubCanary.SecretNeedles)
        {
            Assert.DoesNotContain(needle, text, StringComparison.Ordinal);
        }
    }

    private async Task SeedAsync(string? planXml)
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
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 10, 500000, 1000000, 1000, 0, $10, $11)";
        cmd.Parameters.Add(new DuckDBParameter { Value = 1L });
        cmd.Parameters.Add(new DuckDBParameter { Value = WindowEnd.AddMinutes(-30) });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SsfDb" });
        cmd.Parameters.Add(new DuckDBParameter { Value = Hash });
        cmd.Parameters.Add(new DuckDBParameter { Value = "0xSSF0LITEPLAN" });
        cmd.Parameters.Add(new DuckDBParameter { Value = PlanHandle });
        cmd.Parameters.Add(new DuckDBParameter { Value = WindowEnd.AddDays(-1) });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SELECT * FROM dbo.SsfLiteTable" });
        cmd.Parameters.Add(new DuckDBParameter { Value = planXml is null ? DBNull.Value : planXml });
        await cmd.ExecuteNonQueryAsync();
    }
}
