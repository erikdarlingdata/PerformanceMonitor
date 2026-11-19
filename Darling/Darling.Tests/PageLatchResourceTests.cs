/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9055: the shared page latch helper both apps' get_active_queries call. What a page latch wait is on comes from the request's own
/// wait_resource, its stored plan and the newest daily index snapshot, and these tests pin the answer, not the helper's wording.
/// </summary>
public sealed class PageLatchResourceTests
{
    [Theory]
    [InlineData(1, 1L, "PFS")]
    [InlineData(1, 8088L, "PFS")]
    [InlineData(1, 16176L, "PFS")]
    [InlineData(1, 2L, "GAM")]
    [InlineData(1, 511232L, "GAM")]
    [InlineData(1, 3L, "SGAM")]
    [InlineData(1, 511233L, "SGAM")]
    [InlineData(1, 6L, "DCM")]
    [InlineData(1, 511238L, "DCM")]
    [InlineData(3, 1022470L, "DCM")]
    [InlineData(1, 7L, "BCM")]
    [InlineData(1, 511239L, "BCM")]
    [InlineData(3, 1022471L, "BCM")]
    [InlineData(1, 9L, "boot page")]
    [InlineData(2, 9L, "data or index page")]
    [InlineData(1, 4L, "data or index page")]
    [InlineData(1, 8L, "data or index page")]
    [InlineData(1, 8089L, "data or index page")]
    [InlineData(1, 511234L, "data or index page")]
    [InlineData(1, 511240L, "data or index page")]
    [InlineData(1, 0L, "file header")]
    public void PageKind_NamesTheFixedAllocationAndHeaderPagesByPageNumber(int fileId, long pageId, string expected) =>
        Assert.Equal(expected, PageLatchResource.PageKind(fileId, pageId));

    [Theory]
    [InlineData("PAGELATCH_EX", "5:1:6", "DCM")]
    [InlineData("PAGELATCH_UP", "5:2:511239", "BCM")]
    [InlineData("PAGELATCH_SH", "5:1:9", "boot page")]
    public void Describe_DcmBcmAndBootPage_NameTheKind_AndNoIndexesEvenWhenThePlanNamesOne(string waitType, string resource, string kind)
    {
        var answer = PageLatchResource.Describe(waitType, resource, SeekPlan, Stats(), Snapshot)!;

        Assert.Equal(kind, answer.PageKind);
        Assert.Null(answer.Indexes);
        Assert.Null(answer.Match);
    }

    [Fact]
    public void Describe_PageNineOfAnotherFile_IsAnOrdinaryPage_SoThePlanStillNamesItsIndex()
    {
        var answer = PageLatchResource.Describe("PAGELATCH_SH", "5:2:9", SeekPlan, Stats(), Snapshot)!;

        Assert.Equal("data or index page", answer.PageKind);
        Assert.Equal("exact", answer.Match);
    }

    [Fact]
    public void Describe_IndexTheSnapshotHasNoRowFor_HasNullWaits_NotZero()
    {
        var plan = Plan(
            $"<RelOp NodeId=\"0\" PhysicalOp=\"Clustered Index Insert\" LogicalOp=\"Insert\" {RelOpAttributes}><OutputList/>"
            + "<Update DMLRequestSort=\"0\">"
            + "<Object Database=\"[Shop]\" Schema=\"[dbo]\" Table=\"[Orders]\" Index=\"[PK_Orders]\" IndexKind=\"Clustered\" Storage=\"RowStore\"/>"
            + "<Object Database=\"[Shop]\" Schema=\"[dbo]\" Table=\"[Orders]\" Index=\"[IX_New]\" IndexKind=\"NonClustered\" Storage=\"RowStore\"/>"
            + "</Update></RelOp>");
        var answer = PageLatchResource.Describe("PAGELATCH_EX", "5:1:4096", plan, Stats(), Snapshot)!;

        Assert.Equal("plan", answer.Match);
        Assert.Equal(["PK_Orders", "IX_New"], answer.Indexes!.Select(i => i.IndexName).ToArray());
        var missing = answer.Indexes![1];
        Assert.Null(missing.WaitMs);
        Assert.Null(missing.WaitCount);
        var json = answer.ToJson()["indexes"]!.AsArray()[1]!.AsObject();
        Assert.Null(json["latch_wait_ms"]);
        Assert.Null(json["latch_wait_count"]);
        Assert.Equal(100L, answer.Indexes![0].WaitMs);
    }

    [Fact]
    public void Describe_IndexWithNoWaitsInTheSnapshot_StaysZero()
    {
        var answer = PageLatchResource.Describe("PAGELATCH_EX", "5:1:4096", Plan(Seek("Shop", "Quiet", "PK_Quiet", "Clustered")), Stats(), Snapshot)!;

        var index = Assert.Single(answer.Indexes!);
        Assert.Equal((0L, 0L), (index.WaitMs, index.WaitCount));
    }

    [Theory]
    [InlineData("5:1:4096", 5, 1, 4096L)]
    [InlineData(" 12:3:511233 ", 12, 3, 511233L)]
    public void TryParse_AcceptsThreeIntegers(string resource, int database, int file, long page)
    {
        Assert.True(PageLatchResource.TryParse(resource, out var d, out var f, out var p));
        Assert.Equal((database, file, page), (d, f, p));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("KEY: 5:72057594038779904 (a1b2c3d4e5f6)")]
    [InlineData("5:1")]
    [InlineData("5:1:2:3")]
    [InlineData("a:b:c")]
    [InlineData("5:-1:4")]
    public void TryParse_RejectsAnythingElse(string? resource) =>
        Assert.False(PageLatchResource.TryParse(resource, out _, out _, out _));

    [Fact]
    public void Describe_JunkWaitResource_AndNonPageLatchWait_GiveNull()
    {
        Assert.Null(PageLatchResource.Describe("PAGELATCH_EX", "KEY: 5:7 (aa)", null, Stats(), Snapshot));
        Assert.Null(PageLatchResource.Describe("PAGELATCH_EX", null, null, Stats(), Snapshot));
        Assert.Null(PageLatchResource.Describe("LCK_M_S", "5:1:4096", null, Stats(), Snapshot));
        Assert.Null(PageLatchResource.Describe(null, "5:1:4096", null, Stats(), Snapshot));
    }

    [Fact]
    public void Describe_AllocationPage_NamesTheKindAndNoIndexes()
    {
        var answer = PageLatchResource.Describe("PAGELATCH_UP", "5:1:2", null, Stats(), Snapshot)!;

        Assert.Equal("GAM", answer.PageKind);
        Assert.Null(answer.Indexes);
        Assert.Null(answer.Match);
        Assert.Null(answer.IndexSnapshotTime);
    }

    [Fact]
    public void Describe_PlanWithOneIndex_IsExact_WithThatIndexsWaits()
    {
        var answer = PageLatchResource.Describe("PAGELATCH_SH", "5:1:4096", SeekPlan, Stats(), Snapshot)!;

        Assert.Equal("exact", answer.Match);
        var index = Assert.Single(answer.Indexes!);
        Assert.Equal(("dbo", "Orders", "IX_Orders_Date", (long?)400L, (long?)40L), (index.SchemaName, index.TableName, index.IndexName, index.WaitMs, index.WaitCount));
        Assert.Equal(Snapshot, answer.IndexSnapshotTime);
    }

    [Fact]
    public void Describe_InsertThatMaintainsAnIndex_IsPlan_WithBothIndexes_HighestWaitFirst()
    {
        var answer = PageLatchResource.Describe("PAGELATCH_EX", "5:1:4096", InsertPlan, Stats(), Snapshot)!;

        Assert.Equal("plan", answer.Match);
        Assert.Equal(["IX_Orders_Date", "PK_Orders"], answer.Indexes!.Select(i => i.IndexName).ToArray());
        Assert.Equal<long?>([400L, 100L], answer.Indexes!.Select(i => i.WaitMs).ToArray());
    }

    [Fact]
    public void Describe_PageIoLatchRow_ReadsThePageIoLatchColumns()
    {
        var answer = PageLatchResource.Describe("PAGEIOLATCH_SH", "5:1:4096", SeekPlan, Stats(), Snapshot)!;

        var index = Assert.Single(answer.Indexes!);
        Assert.Equal(((long?)70L, (long?)7L), (index.WaitMs, index.WaitCount));
        Assert.Equal("page_io_latch", answer.ToJson()["latch_kind"]!.GetValue<string>());
    }

    [Fact]
    public void Describe_ObjectInAnotherDatabase_IsLeftOut_AndTheFallbackTakesOver()
    {
        var answer = PageLatchResource.Describe("PAGELATCH_EX", "5:1:4096", OtherDatabasePlan, Stats(), Snapshot)!;

        Assert.Equal("inferred", answer.Match);
        Assert.DoesNotContain(answer.Indexes!, i => i.TableName == "OtherTable");
    }

    [Fact]
    public void Describe_PlanWithOneObjectInAnotherDatabase_AndOneHere_NamesOnlyTheOneHere()
    {
        var plan = Plan(Seek("Shop", "Orders", "IX_Orders_Date", "NonClustered") + Seek("Other", "OtherTable", "PK_OtherTable", "Clustered"));
        var answer = PageLatchResource.Describe("PAGELATCH_EX", "5:1:4096", plan, Stats(), Snapshot)!;

        Assert.Equal("exact", answer.Match);
        Assert.Equal("IX_Orders_Date", Assert.Single(answer.Indexes!).IndexName);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("<not a plan")]
    public void Describe_NoPlan_OrAnUnreadableOne_FallsBackToTheTopIndexesByWaits(string? plan)
    {
        var answer = PageLatchResource.Describe("PAGELATCH_EX", "5:1:4096", plan, Stats(), Snapshot)!;

        Assert.Equal("inferred", answer.Match);
        Assert.Equal(["Customers.PK_Customers", "Orders.IX_Orders_Date", "Orders.PK_Orders"], answer.Indexes!.Select(i => $"{i.TableName}.{i.IndexName}").ToArray());
        Assert.Equal<long?>([900L, 400L, 100L], answer.Indexes!.Select(i => i.WaitMs).ToArray());
        Assert.Equal(PageLatchResource.InferredIndexCount, answer.Indexes!.Count);
    }

    [Fact]
    public void Describe_PlanNamingAHeap_ListsTheHeapWithANullIndexName()
    {
        var answer = PageLatchResource.Describe("PAGELATCH_EX", "5:1:4096", Plan(HeapScan("Shop", "Audit")), Stats(), Snapshot)!;

        Assert.Equal("exact", answer.Match);
        var heap = Assert.Single(answer.Indexes!);
        Assert.Equal(("Audit", (string?)null, (long?)30L), (heap.TableName, heap.IndexName, heap.WaitMs));
    }

    [Fact]
    public void Describe_TempdbDataPage_HasANote_AndNoIndexes()
    {
        var answer = PageLatchResource.Describe("PAGELATCH_EX", "2:1:4096", SeekPlan, Stats(), Snapshot)!;

        Assert.Equal("data or index page", answer.PageKind);
        Assert.Null(answer.Indexes);
        Assert.Contains("tempdb", answer.Note, StringComparison.Ordinal);
    }

    [Fact]
    public void Describe_NoIndexSnapshot_AndADatabaseWithNoRows_NeitherInventsIndexes()
    {
        Assert.Null(PageLatchResource.Describe("PAGELATCH_EX", "5:1:4096", SeekPlan, null, null)!.Indexes);
        Assert.Null(PageLatchResource.Describe("PAGELATCH_EX", "9:1:4096", SeekPlan, Stats(), Snapshot)!.Indexes);
    }

    [Fact]
    public void ToJson_CarriesThePageAndTheIndexesAndTheSnapshotTime()
    {
        var node = PageLatchResource.Describe("PAGELATCH_EX", "5:1:4096", InsertPlan, Stats(), Snapshot)!.ToJson();

        Assert.Equal(5, node["database_id"]!.GetValue<int>());
        Assert.Equal(4096L, node["page_id"]!.GetValue<long>());
        Assert.Equal("data or index page", node["page_kind"]!.GetValue<string>());
        Assert.Equal("plan", node["match"]!.GetValue<string>());
        Assert.Equal(2, node["indexes"]!.AsArray().Count);
        Assert.Equal(Snapshot, DateTime.Parse(node["index_snapshot_time"]!.GetValue<string>(), null, System.Globalization.DateTimeStyles.RoundtripKind));
    }

    private static readonly DateTime Snapshot = new(2026, 11, 18, 3, 0, 0, DateTimeKind.Utc);

    /* Database 5 "Shop" has the waits; database 6 "Other" has a bigger one a Shop page must never pick up. */
    private static PageLatchIndexStat[] Stats() =>
    [
        new(5, "Shop", "dbo", "Orders", "PK_Orders", 100, 10, 5, 1),
        new(5, "Shop", "dbo", "Orders", "IX_Orders_Date", 400, 40, 70, 7),
        new(5, "Shop", "dbo", "Customers", "PK_Customers", 900, 90, 0, 0),
        new(5, "Shop", "dbo", "Items", "PK_Items", 20, 2, 0, 0),
        new(5, "Shop", "dbo", "Audit", null, 30, 3, 0, 0),
        new(5, "Shop", "dbo", "Quiet", "PK_Quiet", 0, 0, 0, 0),
        new(6, "Other", "dbo", "OtherTable", "PK_OtherTable", 5000, 500, 0, 0),
    ];

    internal static string Plan(string relOps) =>
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.5\" Build=\"16.0.1000.6\"><BatchSequence><Batch><Statements>"
        + "<StmtSimple StatementText=\"x\" StatementId=\"1\" StatementType=\"SELECT\"><QueryPlan CachedPlanSize=\"16\">" + relOps + "</QueryPlan></StmtSimple>"
        + "</Statements></Batch></BatchSequence></ShowPlanXML>";

    private const string RelOpAttributes = "EstimateRows=\"1\" EstimateIO=\"0.003\" EstimateCPU=\"0.0001\" AvgRowSize=\"9\" EstimatedTotalSubtreeCost=\"0.003\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\"";

    internal static string Seek(string database, string table, string index, string kind) =>
        $"<RelOp NodeId=\"0\" PhysicalOp=\"Index Seek\" LogicalOp=\"Index Seek\" {RelOpAttributes}><OutputList/>"
        + "<IndexScan Ordered=\"1\" ScanDirection=\"FORWARD\" ForcedIndex=\"0\" ForceSeek=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\">"
        + $"<Object Database=\"[{database}]\" Schema=\"[dbo]\" Table=\"[{table}]\" Index=\"[{index}]\" IndexKind=\"{kind}\" Storage=\"RowStore\"/></IndexScan></RelOp>";

    private static string HeapScan(string database, string table) =>
        $"<RelOp NodeId=\"0\" PhysicalOp=\"Table Scan\" LogicalOp=\"Table Scan\" {RelOpAttributes}><OutputList/>"
        + "<TableScan Ordered=\"0\" ForcedIndex=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\">"
        + $"<Object Database=\"[{database}]\" Schema=\"[dbo]\" Table=\"[{table}]\" IndexKind=\"Heap\" Storage=\"RowStore\"/></TableScan></RelOp>";

    internal static readonly string SeekPlan = Plan(Seek("Shop", "Orders", "IX_Orders_Date", "NonClustered"));

    internal static readonly string OtherDatabasePlan = Plan(Seek("Other", "OtherTable", "PK_OtherTable", "Clustered"));

    /* A clustered index insert: the first Object is the clustered index, the second the nonclustered index it maintains. */
    internal static readonly string InsertPlan = Plan(
        $"<RelOp NodeId=\"0\" PhysicalOp=\"Clustered Index Insert\" LogicalOp=\"Insert\" {RelOpAttributes}><OutputList/>"
        + "<Update DMLRequestSort=\"0\">"
        + "<Object Database=\"[Shop]\" Schema=\"[dbo]\" Table=\"[Orders]\" Index=\"[PK_Orders]\" IndexKind=\"Clustered\" Storage=\"RowStore\"/>"
        + "<Object Database=\"[Shop]\" Schema=\"[dbo]\" Table=\"[Orders]\" Index=\"[IX_Orders_Date]\" IndexKind=\"NonClustered\" Storage=\"RowStore\"/>"
        + "</Update></RelOp>");
}
