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
using PerformanceMonitor.Common;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8147 - the shared plan analysis envelope states which statistics the optimizer loaded while compiling each
/// statement (<c>statistics_loaded</c>), how many there were, and whether the plan recorded them at all. The
/// plans are built per assertion; the expected values are facts of the fixture.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class McpPlanAnalysisStatisticsLoadedTests
{
    private static readonly string[] RowFields =
    {
        "database", "schema", "table", "statistic", "modification_count", "table_rows", "modified_percent", "stale_at_compile", "last_update", "sampling_percent"
    };

    private sealed record Stat(string Table, string Name, long Mods, string Db = "SalesDb", string Schema = "dbo", string? LastUpdate = "2026-10-30T02:14:07.53", string Sampling = "100");

    private sealed record Scan(string Table, string Cardinality, string Db = "SalesDb", string Schema = "dbo");

    private static string StatsXml(IEnumerable<Stat> stats) =>
        "<OptimizerStatsUsage>" + string.Concat(stats.Select(s =>
            $"<StatisticsInfo Database=\"{s.Db}\" Schema=\"{s.Schema}\" Table=\"{s.Table}\" Statistics=\"{s.Name}\" ModificationCount=\"{s.Mods}\" SamplingPercent=\"{s.Sampling}\"" +
            (s.LastUpdate == null ? "" : $" LastUpdate=\"{s.LastUpdate}\"") + "/>")) + "</OptimizerStatsUsage>";

    private static string ScanRelOp(int id, Scan s) =>
        $"<RelOp NodeId=\"{id}\" PhysicalOp=\"Table Scan\" LogicalOp=\"Table Scan\" EstimateRows=\"1\" EstimateIO=\"0.003\" EstimateCPU=\"0.0001\" AvgRowSize=\"9\" EstimatedTotalSubtreeCost=\"0.0032\" TableCardinality=\"{s.Cardinality}\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">" +
        "<OutputList/><TableScan Ordered=\"0\" ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\"><DefinedValues/>" +
        $"<Object Database=\"[{s.Db}]\" Schema=\"[{s.Schema}]\" Table=\"[{s.Table}]\" IndexKind=\"Heap\" Storage=\"RowStore\"/></TableScan></RelOp>";

    private static string QueryPlan(string? statsXml, IReadOnlyList<Scan> scans)
    {
        var body = scans.Count == 1
            ? ScanRelOp(0, scans[0])
            : "<RelOp NodeId=\"0\" PhysicalOp=\"Nested Loops\" LogicalOp=\"Inner Join\" EstimateRows=\"1\" EstimateIO=\"0\" EstimateCPU=\"0.1\" AvgRowSize=\"9\" EstimatedTotalSubtreeCost=\"0.5\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\"><OutputList/><NestedLoops Optimized=\"0\">" +
              string.Concat(scans.Select((s, i) => ScanRelOp(i + 1, s))) + "</NestedLoops></RelOp>";
        return "<QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"64\">" + statsXml + body + "</QueryPlan>";
    }

    private static string SelectStmt(string? statsXml, params Scan[] scans) =>
        "<StmtSimple StatementText=\"SELECT * FROM dbo.Orders\" StatementId=\"1\" StatementType=\"SELECT\" StatementSubTreeCost=\"1\" StatementEstRows=\"1\">" +
        QueryPlan(statsXml, scans.Length == 0 ? new[] { new Scan("Orders", "1000") } : scans) + "</StmtSimple>";

    private static string Wrap(string statements) =>
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.539\" Build=\"16.0.1000.6\">" +
        "<BatchSequence><Batch><Statements>" + statements + "</Statements></Batch></BatchSequence></ShowPlanXML>";

    private static string Result(string xml) => McpPlanAnalysisFormatter.BuildAnalysisResult(xml, "s", "xml", null);

    private static JsonDocument Run(string? statsXml, params Scan[] scans) => JsonDocument.Parse(Result(Wrap(SelectStmt(statsXml, scans))));

    private static JsonElement OnlyStatement(JsonDocument doc) => Assert.Single(doc.RootElement.GetProperty("statements").EnumerateArray());

    private static List<JsonElement> Rows(JsonElement stmt) => stmt.GetProperty("statistics_loaded").EnumerateArray().ToList();

    [Fact]
    public void StatsLoaded_TwelveStatistics_AreOrderedCappedAndTruncationIsObserved()
    {
        var counts = new long[] { 7, 900, 3, 450, 12, 5000, 1, 88, 640, 20, 2, 130 };
        var stats = counts.Select((c, i) => new Stat("Orders", $"ST_{i:00}", c));
        using var doc = Run(StatsXml(stats));
        var stmt = OnlyStatement(doc);

        Assert.Equal("recorded", stmt.GetProperty("statistics_status").GetString());
        Assert.Equal(McpPlanAnalysisFormatter.StatisticsLoadedCap, stmt.GetProperty("statistics_cap").GetInt32());
        Assert.Equal(10, stmt.GetProperty("statistics_cap").GetInt32());
        Assert.Equal(10, stmt.GetProperty("statistics_returned").GetInt32());
        Assert.Equal(12, stmt.GetProperty("total_statistics").GetInt32());
        Assert.True(stmt.GetProperty("statistics_truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, stmt.GetProperty("statistics_note").ValueKind);

        var rows = Rows(stmt);
        Assert.Equal(10, rows.Count);
        var mods = rows.Select(r => r.GetProperty("modification_count").GetInt64()).ToList();
        Assert.Equal(counts.OrderByDescending(c => c).Take(10).ToList(), mods);
    }

    [Fact]
    public void StatsLoaded_TenStatistics_IsNotTruncated()
    {
        using var doc = Run(StatsXml(Enumerable.Range(0, 10).Select(i => new Stat("Orders", $"ST_{i:00}", i))));
        var stmt = OnlyStatement(doc);

        Assert.Equal(10, stmt.GetProperty("statistics_returned").GetInt32());
        Assert.Equal(10, stmt.GetProperty("total_statistics").GetInt32());
        Assert.False(stmt.GetProperty("statistics_truncated").GetBoolean());
        Assert.Equal(10, Rows(stmt).Count);
    }

    [Fact]
    public void StatsLoaded_EqualCounts_AreOrderedByName()
    {
        var stats = new[]
        {
            new Stat("Orders", "ST_b", 5, Db: "SalesDb", Schema: "dbo"),
            new Stat("Orders", "ST_a", 5, Db: "SalesDb", Schema: "dbo"),
            new Stat("Orders", "ST_a", 5, Db: "SalesDb", Schema: "arch"),
            new Stat("Customers", "ST_z", 5, Db: "SalesDb", Schema: "dbo"),
            new Stat("Orders", "ST_a", 5, Db: "AuditDb", Schema: "dbo"),
        };
        using var doc = Run(StatsXml(stats));
        var rows = Rows(OnlyStatement(doc));

        var keys = rows.Select(r => string.Join("|", r.GetProperty("database").GetString(), r.GetProperty("schema").GetString(), r.GetProperty("table").GetString(), r.GetProperty("statistic").GetString())).ToList();
        Assert.Equal(new[]
        {
            "AuditDb|dbo|Orders|ST_a",
            "SalesDb|arch|Orders|ST_a",
            "SalesDb|dbo|Customers|ST_z",
            "SalesDb|dbo|Orders|ST_a",
            "SalesDb|dbo|Orders|ST_b",
        }, keys);
    }

    [Fact]
    public void StatsLoaded_RowShare_FromTheScansTableCardinality()
    {
        using var doc = Run(StatsXml(new[] { new Stat("Orders", "IX_Orders_CustomerId", 250) }), new Scan("Orders", "1000"));
        var row = Assert.Single(Rows(OnlyStatement(doc)));

        Assert.Equal(1000, row.GetProperty("table_rows").GetDouble());
        Assert.Equal(25.0, row.GetProperty("modified_percent").GetDouble());
        Assert.Equal(250, row.GetProperty("modification_count").GetInt64());
        Assert.Equal(100, row.GetProperty("sampling_percent").GetDouble());
    }

    [Fact]
    public void StatsLoaded_TableNoOperatorReads_HasNullRowsAndPercent()
    {
        using var doc = Run(StatsXml(new[] { new Stat("Customers", "ST_c", 40) }), new Scan("Orders", "1000"));
        var row = Assert.Single(Rows(OnlyStatement(doc)));

        Assert.Equal(JsonValueKind.Null, row.GetProperty("table_rows").ValueKind);
        Assert.Equal(JsonValueKind.Null, row.GetProperty("modified_percent").ValueKind);
        Assert.Equal(40, row.GetProperty("modification_count").GetInt64());
    }

    [Fact]
    public void StatsLoaded_ThreeOperatorsOnOneTable_TakesTheLargestCardinality()
    {
        using var doc = Run(StatsXml(new[] { new Stat("Orders", "ST_o", 100) }), new Scan("Orders", "400"), new Scan("Orders", "800"), new Scan("Orders", "600"));
        var row = Assert.Single(Rows(OnlyStatement(doc)));

        Assert.Equal(800, row.GetProperty("table_rows").GetDouble());
        Assert.Equal(12.5, row.GetProperty("modified_percent").GetDouble());
    }

    [Fact]
    public void StatsLoaded_OperatorWithoutTableCardinality_GivesNullRowsAndPercent()
    {
        var update =
            "<StmtSimple StatementText=\"UPDATE dbo.Orders SET Status = 2 WHERE OrderId = 7\" StatementId=\"1\" StatementType=\"UPDATE\" StatementSubTreeCost=\"0.01\" StatementEstRows=\"1\">" +
            "<QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"64\">" +
            StatsXml(new[] { new Stat("Orders", "PK_Orders", 300) }) +
            "<RelOp NodeId=\"0\" PhysicalOp=\"Clustered Index Update\" LogicalOp=\"Clustered Index Update\" EstimateRows=\"1\" EstimateIO=\"0.01\" EstimateCPU=\"0.0001\" AvgRowSize=\"9\" EstimatedTotalSubtreeCost=\"0.01\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">" +
            "<OutputList/><Update DMLRequestSort=\"0\" ActionColumn=\"[Action1004]\"><Object Database=\"[SalesDb]\" Schema=\"[dbo]\" Table=\"[Orders]\" Index=\"[PK_Orders]\" IndexKind=\"Clustered\" Storage=\"RowStore\"/></Update></RelOp>" +
            "</QueryPlan></StmtSimple>";
        using var doc = JsonDocument.Parse(Result(Wrap(update)));
        var row = Assert.Single(Rows(OnlyStatement(doc)));

        Assert.Equal(JsonValueKind.Null, row.GetProperty("table_rows").ValueKind);
        Assert.Equal(JsonValueKind.Null, row.GetProperty("modified_percent").ValueKind);
        Assert.Equal(300, row.GetProperty("modification_count").GetInt64());
    }

    private const string TempRaw = "#orders_______________________________________________________________________________________________000000000012";

    [Fact]
    public void StatsLoaded_TempTable_ShowsItsShortNameAndGetsItsRowCount()
    {
        using var doc = Run(StatsXml(new[] { new Stat(TempRaw, "_WA_Sys_00000001_0", 40, Db: "tempdb") }), new Scan(TempRaw, "200", Db: "tempdb"));
        var row = Assert.Single(Rows(OnlyStatement(doc)));

        Assert.Equal("#orders", row.GetProperty("table").GetString());
        Assert.Equal(200, row.GetProperty("table_rows").GetDouble());
        Assert.Equal(20.0, row.GetProperty("modified_percent").GetDouble());
    }

    private static string IfStmt(string? statsXml) =>
        "<StmtCond StatementText=\"IF EXISTS (SELECT 1 FROM dbo.Orders)\" StatementId=\"1\" StatementType=\"COND WITH QUERY\" StatementSubTreeCost=\"0.0032\">" +
        "<Condition>" + QueryPlan(statsXml, new[] { new Scan("Orders", "1000") }) + "</Condition><Then><Statements/></Then></StmtCond>";

    [Fact]
    public void StatsLoaded_IfConditionWithRecordedStats_IsRecorded()
    {
        using var doc = JsonDocument.Parse(Result(Wrap(IfStmt(StatsXml(new[] { new Stat("Orders", "IX_Orders_CustomerId", 250) })))));
        var stmt = OnlyStatement(doc);
        var row = Assert.Single(Rows(stmt));

        Assert.Equal("recorded", stmt.GetProperty("statistics_status").GetString());
        Assert.Equal("IX_Orders_CustomerId", row.GetProperty("statistic").GetString());
        Assert.Equal(1000, row.GetProperty("table_rows").GetDouble());
        Assert.Equal(25.0, row.GetProperty("modified_percent").GetDouble());
    }

    [Fact]
    public void StatsLoaded_IfConditionWithoutStatsUsage_IsNotInPlan()
    {
        using var doc = JsonDocument.Parse(Result(Wrap(IfStmt(null))));
        var stmt = OnlyStatement(doc);

        Assert.Equal("not_in_plan", stmt.GetProperty("statistics_status").GetString());
        Assert.Equal(McpPlanAnalysisFormatter.StatisticsNotInPlanNote, stmt.GetProperty("statistics_note").GetString());
        Assert.Empty(Rows(stmt));
    }

    [Fact]
    public void StatsLoaded_ZeroTableRows_GivesNullPercent()
    {
        using var doc = Run(StatsXml(new[] { new Stat("Orders", "ST_o", 100) }), new Scan("Orders", "0"));
        var row = Assert.Single(Rows(OnlyStatement(doc)));

        Assert.Equal(0, row.GetProperty("table_rows").GetDouble());
        Assert.Equal(JsonValueKind.Null, row.GetProperty("modified_percent").ValueKind);
    }

    [Fact]
    public void StatsLoaded_MatchesNamesIgnoringCaseAndBrackets()
    {
        using var doc = Run(
            StatsXml(new[] { new Stat("[orders]", "[ST_o]", 50, Db: "[salesdb]", Schema: "[DBO]") }),
            new Scan("ORDERS", "500", Db: "SALESDB", Schema: "dbo"));
        var row = Assert.Single(Rows(OnlyStatement(doc)));

        Assert.Equal(500, row.GetProperty("table_rows").GetDouble());
        Assert.Equal(10.0, row.GetProperty("modified_percent").GetDouble());
        Assert.Equal("orders", row.GetProperty("table").GetString());
        Assert.Equal("ST_o", row.GetProperty("statistic").GetString());
    }

    [Fact]
    public void StatsLoaded_MissingLastUpdate_IsNull()
    {
        using var doc = Run(StatsXml(new[] { new Stat("Orders", "ST_o", 1, LastUpdate: null) }));
        var row = Assert.Single(Rows(OnlyStatement(doc)));

        Assert.Equal(JsonValueKind.Null, row.GetProperty("last_update").ValueKind);
    }

    [Fact]
    public void StatsLoaded_LastUpdate_IsVerbatim()
    {
        using var doc = Run(StatsXml(new[] { new Stat("Orders", "ST_o", 1, LastUpdate: "2026-10-30T02:14:07.53") }));
        var row = Assert.Single(Rows(OnlyStatement(doc)));

        Assert.Equal("2026-10-30T02:14:07.53", row.GetProperty("last_update").GetString());
    }

    [Fact]
    public void StatsLoaded_PlanWithoutStatsUsage_IsNotInPlanWithTheNote()
    {
        using var doc = Run(null);
        var stmt = OnlyStatement(doc);

        Assert.Equal("not_in_plan", stmt.GetProperty("statistics_status").GetString());
        Assert.Equal(McpPlanAnalysisFormatter.StatisticsNotInPlanNote, stmt.GetProperty("statistics_note").GetString());
        Assert.Empty(Rows(stmt));
        Assert.Equal(0, stmt.GetProperty("statistics_returned").GetInt32());
        Assert.Equal(0, stmt.GetProperty("total_statistics").GetInt32());
        Assert.False(stmt.GetProperty("statistics_truncated").GetBoolean());
        Assert.Equal(10, stmt.GetProperty("statistics_cap").GetInt32());
    }

    [Fact]
    public void StatsLoaded_EmptyRecordedList_HasNoNote()
    {
        using var doc = Run("<OptimizerStatsUsage/>");
        var stmt = OnlyStatement(doc);

        Assert.Equal("recorded", stmt.GetProperty("statistics_status").GetString());
        Assert.Equal(JsonValueKind.Null, stmt.GetProperty("statistics_note").ValueKind);
        Assert.Empty(Rows(stmt));
        Assert.Equal(0, stmt.GetProperty("total_statistics").GetInt32());
        Assert.False(stmt.GetProperty("statistics_truncated").GetBoolean());
    }

    [Fact]
    public void StatsLoaded_ProcedurePlan_DeclareIsNoQueryPlan_InnerStatementIsRecorded()
    {
        var inner = SelectStmt(StatsXml(new[] { new Stat("Orders", "ST_o", 9) }));
        var xml = Wrap(
            "<StmtSimple StatementText=\"EXEC dbo.GetOrders\" StatementId=\"1\" StatementType=\"EXEC PROC\">" +
            $"<StoredProc ProcName=\"[dbo].[GetOrders]\"><Statements><StmtSimple StatementText=\"DECLARE @i int\" StatementId=\"2\" StatementType=\"DECLARE\"/>{inner}</Statements></StoredProc>" +
            "</StmtSimple>");
        using var doc = JsonDocument.Parse(Result(xml));
        var statements = doc.RootElement.GetProperty("statements").EnumerateArray().ToList();

        var recorded = Assert.Single(statements, s => s.GetProperty("statistics_status").GetString() == "recorded");
        Assert.Equal("ST_o", Assert.Single(Rows(recorded)).GetProperty("statistic").GetString());

        Assert.Equal(3, statements.Count);
        var planless = statements.Where(s => s.GetProperty("statistics_status").GetString() != "recorded").ToList();
        Assert.Equal(2, planless.Count);
        Assert.Contains(planless, s => s.GetProperty("statement_type").GetString() == "DECLARE");
        Assert.All(planless, s =>
        {
            Assert.Equal("no_query_plan", s.GetProperty("statistics_status").GetString());
            Assert.Equal(JsonValueKind.Null, s.GetProperty("statistics_note").ValueKind);
            Assert.Empty(Rows(s));
            Assert.Equal(10, s.GetProperty("statistics_cap").GetInt32());
            Assert.Equal(0, s.GetProperty("statistics_returned").GetInt32());
            Assert.Equal(0, s.GetProperty("total_statistics").GetInt32());
            Assert.False(s.GetProperty("statistics_truncated").GetBoolean());
        });
    }

    [Theory]
    [InlineData("10000", 2500, false)]
    [InlineData("10000", 2501, true)]
    [InlineData("100", 500, false)]
    [InlineData("100", 501, true)]
    [InlineData("0", 500, false)]
    [InlineData("0", 501, true)]
    [InlineData("500", 500, false)]
    [InlineData("500", 501, true)]
    [InlineData("501", 600, false)]
    [InlineData("501", 601, true)]
    [InlineData("19000", 4300, false)]
    [InlineData("19000", 4301, true)]
    [InlineData("20000", 4472, false)]
    [InlineData("20000", 4473, true)]
    [InlineData("100000", 10000, false)]
    [InlineData("100000", 10001, true)]
    public void StatsLoaded_StaleAtCompile_UsesTheFloorOf500ThenTheSmallerOfTwentyPercentAndSqrtTerms(string rows, int modifications, bool expected)
    {
        using var doc = Run(StatsXml(new[] { new Stat("Orders", "ST_o", modifications) }), new Scan("Orders", rows));

        Assert.Equal(expected, Assert.Single(Rows(OnlyStatement(doc))).GetProperty("stale_at_compile").GetBoolean());
    }

    [Fact]
    public void StatsLoaded_StaleAtCompile_IsNullWhenRowsUnknown()
    {
        using var doc = Run(StatsXml(new[] { new Stat("Customers", "ST_c", 900000) }), new Scan("Orders", "1000"));
        var row = Assert.Single(Rows(OnlyStatement(doc)));

        Assert.Equal(JsonValueKind.Null, row.GetProperty("table_rows").ValueKind);
        Assert.Equal(JsonValueKind.Null, row.GetProperty("stale_at_compile").ValueKind);
    }

    [Fact]
    public void StatsLoaded_RowsCarryOnlyTheSpecifiedFields()
    {
        using var doc = Run(StatsXml(new[] { new Stat("Orders", "ST_o", 9) }));
        var row = Assert.Single(Rows(OnlyStatement(doc)));

        Assert.Equal(RowFields.OrderBy(n => n, StringComparer.Ordinal), row.EnumerateObject().Select(p => p.Name).OrderBy(n => n, StringComparer.Ordinal));
    }

    [Fact]
    public void StatsLoaded_LeavesWarningsAndCountsUnchanged()
    {
        var stats = StatsXml(new[] { new Stat("Orders", "ST_o", 900) });
        using var with = Run(stats);
        using var without = Run(null);

        foreach (var name in new[] { "warning_count", "critical_count" })
        {
            Assert.Equal(OnlyStatement(without).GetProperty(name).GetInt32(), OnlyStatement(with).GetProperty(name).GetInt32());
        }

        Assert.Equal(OnlyStatement(without).GetProperty("warnings").GetRawText(), OnlyStatement(with).GetProperty("warnings").GetRawText());
        foreach (var name in new[] { "total_warnings", "total_critical", "total_missing_indexes" })
        {
            Assert.Equal(without.RootElement.GetProperty(name).GetInt32(), with.RootElement.GetProperty(name).GetInt32());
        }
    }

    [Fact]
    public void StatsLoaded_NamesSurviveMetadataOnlyMasking()
    {
        var result = Result(Wrap(SelectStmt(StatsXml(new[] { new Stat("Orders", "IX_Orders_CustomerId", 9) }))));
        using var masked = JsonDocument.Parse(McpDataDetailMasker.MaskJson(result, McpDataDetail.MetadataOnly));
        var row = Assert.Single(Rows(OnlyStatement(masked)));

        Assert.Equal("IX_Orders_CustomerId", row.GetProperty("statistic").GetString());
        Assert.Equal("Orders", row.GetProperty("table").GetString());
    }
}
