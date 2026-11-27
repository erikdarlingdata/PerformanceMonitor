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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// DB-free tests for the shared <see cref="ObjectUsageReport"/> (#8784), the reply both products' <c>get_object_usage</c> return: the
/// classification of tables and modules, decommission candidates first, a module listed as never run only where an object list exists, the
/// objects the evidence cannot show (memory-optimized tables, inline functions, CLR modules), names matched exactly, the primary and its
/// readable secondary read together (tables summed, executions not), the covered days measured against the window, paging and the
/// evidence cache. No database.
/// </summary>
public sealed class ObjectUsageReportTests
{
    private static readonly DateTime Now = new(2026, 11, 14, 12, 0, 0);
    private static readonly DateTime WindowStart = Now.AddDays(-90);

    private static ObjectUsageWindowResult Tables(DateTime from, DateTime to, params ObjectUsageTotals[] objects) =>
        new(from, to, 91, TimeSpan.FromDays(1), [], ["NODE1"], objects);

    private static ObjectUsageWindowResult TablesOn(string node, params ObjectUsageTotals[] objects) =>
        new(WindowStart, Now, 91, TimeSpan.FromDays(1), [], [node], objects);

    private static ObjectUsageTotals Table(
        string name, long reads = 0, long writes = 0, DateTime? lastRead = null, DateTime? lastWrite = null, DateTime? lastSnapshot = null,
        bool? memoryOptimized = false) =>
        new(name.GetHashCode(StringComparison.Ordinal), "dbo", name, false, reads, writes, lastRead, lastWrite, WindowStart, lastSnapshot ?? Now, 1,
            memoryOptimized);

    private static ObjectUsageSide Side(string server, ObjectUsageWindowResult tables, ObjectExecutionWindow? executions = null, ObjectInventory? inventory = null) =>
        new(server, tables, t => t, executions, inventory);

    private static JsonElement Build(params ObjectUsageSide[] sides) =>
        JsonDocument.Parse(ObjectUsageReport.Build("Sales", 90, WindowStart, Now, sides, null, 100, 0)).RootElement;

    private static JsonElement Obj(JsonElement root, string name) =>
        root.GetProperty("objects").EnumerateArray().Single(o => o.GetProperty("object_name").GetString() == name);

    [Fact]
    public void Tables_AreClassifiedByReadsWritesAndLastSeen()
    {
        var root = Build(Side("srv", Tables(WindowStart, Now,
            Table("Unused"),
            Table("WriteOnly", writes: 5),
            Table("ReadOnly", reads: 3),
            Table("SeenButNotCounted", lastRead: WindowStart.AddDays(10)),
            Table("SeenBeforeWindow", lastRead: WindowStart.AddDays(-10)))));

        Assert.Equal(ObjectUsageReport.NotUsed, Obj(root, "Unused").GetProperty("classification").GetString());
        Assert.Equal(ObjectUsageReport.WriteOnly, Obj(root, "WriteOnly").GetProperty("classification").GetString());
        Assert.Equal(ObjectUsageReport.Read, Obj(root, "ReadOnly").GetProperty("classification").GetString());
        /* A stamp inside the window is evidence of use even when the counted delta is zero (the read fell before the baseline's reset). */
        Assert.Equal(ObjectUsageReport.Read, Obj(root, "SeenButNotCounted").GetProperty("classification").GetString());
        Assert.Equal(ObjectUsageReport.NotUsed, Obj(root, "SeenBeforeWindow").GetProperty("classification").GetString());
    }

    [Fact]
    public void Candidates_ComeFirst()
    {
        var root = Build(Side("srv", Tables(WindowStart, Now, Table("A_Used", reads: 1), Table("B_WriteOnly", writes: 1), Table("C_Unused"))));

        var order = root.GetProperty("objects").EnumerateArray().Select(o => o.GetProperty("object_name").GetString()).ToArray();
        Assert.Equal(new[] { "C_Unused", "B_WriteOnly", "A_Used" }, order);
    }

    [Fact]
    public void WithAnObjectList_AModuleThatNeverRan_IsListedAsNotExecuted()
    {
        var inventory = new ObjectInventory(Now, [
            new ObjectInventoryEntry("dbo", "usp_Daily", "SQL_STORED_PROCEDURE", null, null),
            new ObjectInventoryEntry("dbo", "usp_YearEnd", "SQL_STORED_PROCEDURE", null, null),
            new ObjectInventoryEntry("dbo", "vSomething", "VIEW", null, null),
        ]);
        var executions = new ObjectExecutionWindow(WindowStart, Now, "test", [new ObjectExecutionTotals("dbo.usp_Daily", 90, Now.AddHours(-3))], QueryCaptureMode: "ALL");

        var root = Build(Side("srv", Tables(WindowStart, Now), executions, inventory));

        Assert.Equal(ObjectUsageReport.Executed, Obj(root, "usp_Daily").GetProperty("classification").GetString());
        Assert.Equal(90, Obj(root, "usp_Daily").GetProperty("executions").GetInt64());
        Assert.Equal("SQL_STORED_PROCEDURE", Obj(root, "usp_Daily").GetProperty("object_type").GetString());
        Assert.Equal(ObjectUsageReport.NotExecuted, Obj(root, "usp_YearEnd").GetProperty("classification").GetString());
        /* #8784 part 2: a view is listed, but with no statement text read it is never called unused: there is no evidence either way. */
        Assert.Equal(ObjectUsageReport.NotTracked, Obj(root, "vSomething").GetProperty("classification").GetString());
        Assert.Equal("VIEW", Obj(root, "vSomething").GetProperty("object_type").GetString());
    }

    [Fact]
    public void WithoutAnObjectList_OnlyModulesThatRanAreListed_AndTheNoteSaysSo()
    {
        var executions = new ObjectExecutionWindow(WindowStart, Now, "test", [new ObjectExecutionTotals("dbo.usp_Daily", 4, Now)], QueryCaptureMode: "ALL");

        var root = Build(Side("srv", Tables(WindowStart, Now), executions));

        var module = Obj(root, "usp_Daily");
        Assert.Equal("MODULE", module.GetProperty("object_type").GetString());
        Assert.Equal(ObjectUsageReport.Executed, module.GetProperty("classification").GetString());
        Assert.Contains("NOT listed", root.GetProperty("note").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public void ObjectsQueryStoreCannotSeeRun_AreNotTracked_NotCandidates()
    {
        /* An inline function is expanded into its caller and a CLR module has no T-SQL statements: neither is ever attributed an execution,
           so "never ran" says nothing about them. A scalar function can be inlined on SQL Server 2019 and later, so it stays a candidate but
           carries a caveat. */
        var inventory = new ObjectInventory(Now, [
            new ObjectInventoryEntry("dbo", "fn_GetOrders", "SQL_INLINE_TABLE_VALUED_FUNCTION", null, null),
            new ObjectInventoryEntry("dbo", "clr_Proc", "CLR_STORED_PROCEDURE", null, null),
            new ObjectInventoryEntry("dbo", "clr_Scalar", "CLR_SCALAR_FUNCTION", null, null),
            new ObjectInventoryEntry("dbo", "clr_Tvf", "CLR_TABLE_VALUED_FUNCTION", null, null),
            new ObjectInventoryEntry("dbo", "fn_Scalar", "SQL_SCALAR_FUNCTION", null, null),
            new ObjectInventoryEntry("dbo", "fn_Multi", "SQL_TABLE_VALUED_FUNCTION", null, null),
            new ObjectInventoryEntry("dbo", "usp_Old", "SQL_STORED_PROCEDURE", null, null),
        ]);
        var root = Build(Side("srv", Tables(WindowStart, Now), new ObjectExecutionWindow(WindowStart, Now, "test", [], QueryCaptureMode: "ALL"), inventory));

        foreach (var name in new[] { "fn_GetOrders", "clr_Proc", "clr_Scalar", "clr_Tvf" })
        {
            Assert.Equal(ObjectUsageReport.NotTracked, Obj(root, name).GetProperty("classification").GetString());
            Assert.False(string.IsNullOrEmpty(Obj(root, name).GetProperty("caveat").GetString()));
        }

        Assert.Equal(ObjectUsageReport.NotExecuted, Obj(root, "fn_Scalar").GetProperty("classification").GetString());
        Assert.Equal(ObjectUsageReport.ScalarFunctionCaveat, Obj(root, "fn_Scalar").GetProperty("caveat").GetString());
        Assert.Equal(ObjectUsageReport.NotExecuted, Obj(root, "fn_Multi").GetProperty("classification").GetString());
        Assert.Equal(JsonValueKind.Null, Obj(root, "fn_Multi").GetProperty("caveat").ValueKind);
        Assert.Equal(ObjectUsageReport.NotExecuted, Obj(root, "usp_Old").GetProperty("classification").GetString());

        Assert.Equal(4, root.GetProperty("summary").GetProperty("not_tracked").GetInt32());
        Assert.Equal(3, root.GetProperty("summary").GetProperty("not_executed").GetInt32());

        /* Candidates first, the untracked last. */
        var order = root.GetProperty("objects").EnumerateArray().Select(o => o.GetProperty("classification").GetString()).ToArray();
        Assert.Equal(ObjectUsageReport.NotExecuted, order[0]);
        Assert.Equal(ObjectUsageReport.NotTracked, order[^1]);
    }

    [Fact]
    public void AnUntrackedType_ThatQueryStoreDidSeeRun_IsExecuted()
    {
        var inventory = new ObjectInventory(Now, [new ObjectInventoryEntry("dbo", "fn_GetOrders", "SQL_INLINE_TABLE_VALUED_FUNCTION", null, null)]);
        var executions = new ObjectExecutionWindow(WindowStart, Now, "test", [new ObjectExecutionTotals("dbo.fn_GetOrders", 3, Now)], QueryCaptureMode: "ALL");

        var root = Build(Side("srv", Tables(WindowStart, Now), executions, inventory));

        Assert.Equal(ObjectUsageReport.Executed, Obj(root, "fn_GetOrders").GetProperty("classification").GetString());
    }

    [Fact]
    public void AMemoryOptimizedTable_IsNotTracked_HoweverItsCountersRead()
    {
        /* sys.dm_db_index_usage_stats holds no rows for it, so zero reads and writes is what every memory-optimized table shows. */
        var root = Build(Side("srv", Tables(WindowStart, Now, Table("SessionState", memoryOptimized: true), Table("Unused"))));

        var hot = Obj(root, "SessionState");
        Assert.Equal(ObjectUsageReport.NotTracked, hot.GetProperty("classification").GetString());
        Assert.Equal(ObjectUsageReport.MemoryOptimizedCaveat, hot.GetProperty("caveat").GetString());
        Assert.Equal(1, root.GetProperty("summary").GetProperty("not_used").GetInt32());
        Assert.Equal("Unused", root.GetProperty("objects")[0].GetProperty("object_name").GetString());
    }

    [Fact]
    public void NamesThatDifferOnlyByCase_AreSeparateObjects()
    {
        /* A case-sensitive database holds both. Merged into one row, the abandoned table would hide behind the active one. */
        var inventory = new ObjectInventory(Now, [
            new ObjectInventoryEntry("dbo", "usp_Run", "SQL_STORED_PROCEDURE", null, null),
            new ObjectInventoryEntry("dbo", "USP_RUN", "SQL_STORED_PROCEDURE", null, null),
        ]);
        var executions = new ObjectExecutionWindow(WindowStart, Now, "test", [new ObjectExecutionTotals("dbo.usp_Run", 8, Now)], QueryCaptureMode: "ALL");

        var root = Build(Side("srv", Tables(WindowStart, Now, Table("Orders", reads: 50), Table("ORDERS")), executions, inventory));

        Assert.Equal(ObjectUsageReport.Read, Obj(root, "Orders").GetProperty("classification").GetString());
        Assert.Equal(ObjectUsageReport.NotUsed, Obj(root, "ORDERS").GetProperty("classification").GetString());
        Assert.Equal(ObjectUsageReport.Executed, Obj(root, "usp_Run").GetProperty("classification").GetString());
        Assert.Equal(ObjectUsageReport.NotExecuted, Obj(root, "USP_RUN").GetProperty("classification").GetString());
        Assert.Equal(4, root.GetProperty("summary").GetProperty("objects").GetInt32());
    }

    [Fact]
    public void AModule_IsMatchedToItsTypedRow_WhicheverSideHoldsTheObjectList()
    {
        /* The primary has executions but no object list; the read-only side has the list. The procedure is one row, typed and Executed,
           not a MODULE row beside a typed "Not executed in window" one. */
        var executions = new ObjectExecutionWindow(WindowStart, Now, "test", [new ObjectExecutionTotals("dbo.usp_Daily", 90, Now)], QueryCaptureMode: "ALL");
        var inventory = new ObjectInventory(Now, [new ObjectInventoryEntry("dbo", "usp_Daily", "SQL_STORED_PROCEDURE", null, null)]);
        var primary = Side("SQL1", TablesOn("NODE1"), executions);
        var secondary = Side("SQL1:RO", TablesOn("NODE2"), executions, inventory);

        var root = Build(primary, secondary);

        var row = Obj(root, "usp_Daily");
        Assert.Equal("SQL_STORED_PROCEDURE", row.GetProperty("object_type").GetString());
        Assert.Equal(ObjectUsageReport.Executed, row.GetProperty("classification").GetString());
        Assert.Equal(1, root.GetProperty("summary").GetProperty("objects").GetInt32());
    }

    [Fact]
    public void ModuleExecutions_AreNotSummedAcrossPairedRegistrations()
    {
        /* A database has one Query Store, on the primary: both registrations read the same 1,000 executions. */
        var primary = Side("SQL1", TablesOn("NODE1"),
            new ObjectExecutionWindow(WindowStart, Now, "test", [new ObjectExecutionTotals("dbo.usp_Daily", 1_000, Now.AddHours(-2))], QueryCaptureMode: "ALL"));
        var secondary = Side("SQL1:RO", TablesOn("NODE2"),
            new ObjectExecutionWindow(WindowStart, Now, "test", [new ObjectExecutionTotals("dbo.usp_Daily", 990, Now.AddHours(-5))], QueryCaptureMode: "ALL"));

        var root = Build(primary, secondary);

        var row = Obj(root, "usp_Daily");
        Assert.Equal(1_000, row.GetProperty("executions").GetInt64());
        Assert.Equal(2, row.GetProperty("by_server").GetArrayLength());
        Assert.Contains("not the sum", root.GetProperty("note").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public void RegistrationsAnsweredByTheSameInstance_AreCalledOut()
    {
        var root = Build(
            Side("SQL1", TablesOn("NODE1", Table("Orders", reads: 5))),
            Side("SQL1:RO", TablesOn("node1", Table("Orders", reads: 5))));

        Assert.Contains("WARNING: NODE1 answered more than one", root.GetProperty("note").GetString(), StringComparison.Ordinal);

        var apart = Build(
            Side("SQL1", TablesOn("NODE1", Table("Orders", reads: 5))),
            Side("SQL1:RO", TablesOn("NODE2", Table("Orders", reads: 5))));
        Assert.DoesNotContain("WARNING", apart.GetProperty("note").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public void ExecutionCoverage_CarriesTheDaysWithData_AndTheIndexResets()
    {
        var tables = new ObjectUsageWindowResult(WindowStart, Now, 91, TimeSpan.FromDays(1), [], ["NODE1"], [], IndexResets: 3);
        var root = Build(Side("srv", tables, new ObjectExecutionWindow(WindowStart, Now, "test", [], DaysWithData: 12, QueryCaptureMode: "ALL")));

        var coverage = root.GetProperty("coverage")[0];
        Assert.Equal(12, coverage.GetProperty("executions").GetProperty("days_with_data").GetInt32());
        Assert.Equal(3, coverage.GetProperty("tables").GetProperty("index_resets").GetInt32());
    }

    [Fact]
    public void ADatabaseWithNoQueryStoreData_IsShortOfTheWindow()
    {
        var root = Build(Side("srv", Tables(WindowStart, Now), new ObjectExecutionWindow(null, null, "test", [], DaysWithData: 0, QueryCaptureMode: "ALL")));

        var executions = root.GetProperty("coverage")[0].GetProperty("executions");
        Assert.True(executions.GetProperty("short_of_window").GetBoolean());
        Assert.Equal(0, executions.GetProperty("covered_days").GetDouble());
    }

    [Fact]
    public void TheEvidenceCache_ServesAFreshEntry_DropsAStaleOne_AndStaysBounded()
    {
        long clock = 0;
        var cache = new ObjectUsageEvidenceCache(TimeSpan.FromMinutes(5), () => clock);
        var evidence = new ObjectUsageEvidence(WindowStart, Now, [Side("srv", Tables(WindowStart, Now))]);
        var key = ObjectUsageEvidenceCache.Key([1, 2], "Sales", 90);

        Assert.False(cache.TryGet(key, out _));
        cache.Set(key, evidence);
        clock = 299_000;
        Assert.True(cache.TryGet(key, out var hit));
        Assert.Same(evidence, hit);
        clock = 300_000;
        Assert.False(cache.TryGet(key, out _));

        /* The key tells apart the servers read, the window and the database (exactly: a case-sensitive store has both). */
        Assert.NotEqual(key, ObjectUsageEvidenceCache.Key([1], "Sales", 90));
        Assert.NotEqual(key, ObjectUsageEvidenceCache.Key([1, 2], "Sales", 60));
        Assert.NotEqual(key, ObjectUsageEvidenceCache.Key([1, 2], "sales", 90));

        for (var i = 0; i < ObjectUsageEvidenceCache.MaxEntries + 3; i++)
        {
            clock++;
            cache.Set("k" + i.ToString(System.Globalization.CultureInfo.InvariantCulture), evidence);
        }

        Assert.False(cache.TryGet("k0", out _));
        Assert.True(cache.TryGet("k" + (ObjectUsageEvidenceCache.MaxEntries + 2).ToString(System.Globalization.CultureInfo.InvariantCulture), out _));
    }

    [Fact]
    public void TheWindowRead_SelectsWhatItsMapperReads()
    {
        /* One statement and one mapper for both products: 17 columns, the memory-optimized flag last and read as stored: a NULL (a row from
           before the flag was collected) must reach the mapper as "not known", not as "no". */
        var sql = ObjectUsageWindowRead.Sql;
        var from = sql.IndexOf("s.collection_time,", StringComparison.Ordinal);
        var to = sql.IndexOf("FROM v_index_object_stats AS s", StringComparison.Ordinal);
        var columns = sql[from..to].Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        Assert.Equal(17, columns.Length);
        Assert.Equal("s.is_memory_optimized", columns[^1]);
        Assert.Contains($"interval '{ObjectUsageWindowRead.BaselineReachDays} days'", ObjectUsageWindowRead.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void PrimaryAndReadableSecondary_AreSummed_WithEachSideShown()
    {
        var primary = Side("SQL1", TablesOn("NODE1", Table("Orders", writes: 100), Table("ReportOnly")));
        var secondary = Side("SQL1:RO", TablesOn("NODE2", Table("Orders", reads: 7), Table("ReportOnly", reads: 40)));

        var root = Build(primary, secondary);

        /* ReportOnly looks unused from the primary; the secondary's reads make it used. */
        var reportOnly = Obj(root, "ReportOnly");
        Assert.Equal(ObjectUsageReport.Read, reportOnly.GetProperty("classification").GetString());
        Assert.Equal(40, reportOnly.GetProperty("reads").GetInt64());
        Assert.Equal(2, reportOnly.GetProperty("by_server").GetArrayLength());
        Assert.Equal(2, root.GetProperty("paired_servers").GetArrayLength());
        Assert.Equal(2, root.GetProperty("coverage").GetArrayLength());

        var orders = Obj(root, "Orders");
        Assert.Equal(7, orders.GetProperty("reads").GetInt64());
        Assert.Equal(100, orders.GetProperty("writes").GetInt64());
    }

    [Fact]
    public void SingleSide_HasNoBreakdown()
    {
        var root = Build(Side("srv", Tables(WindowStart, Now, Table("Orders", reads: 1))));

        Assert.Equal(JsonValueKind.Null, root.GetProperty("paired_servers").ValueKind);
        Assert.Equal(JsonValueKind.Null, Obj(root, "Orders").GetProperty("by_server").ValueKind);
    }

    [Fact]
    public void Coverage_ShortOfTheWindow_IsFlagged()
    {
        var root = Build(Side("srv", Tables(Now.AddDays(-20), Now, Table("Orders"))));

        var tables = root.GetProperty("coverage")[0].GetProperty("tables");
        Assert.True(tables.GetProperty("short_of_window").GetBoolean());
        Assert.Equal(20, tables.GetProperty("covered_days").GetDouble());
    }

    [Fact]
    public void ADroppedTable_IsMarked()
    {
        var root = Build(Side("srv", Tables(WindowStart, Now, Table("Gone", lastSnapshot: Now.AddDays(-30)), Table("Here"))));

        Assert.True(Obj(root, "Gone").GetProperty("dropped").GetBoolean());
        Assert.Equal(JsonValueKind.Null, Obj(root, "Here").GetProperty("dropped").ValueKind);
    }

    [Fact]
    public void Paging_ReturnsTheNextOffset()
    {
        var tables = Enumerable.Range(0, 5).Select(i => Table("T" + i)).ToArray();
        var first = JsonDocument.Parse(ObjectUsageReport.Build("Sales", 90, WindowStart, Now, [Side("srv", Tables(WindowStart, Now, tables))], null, 2, 0)).RootElement;

        Assert.Equal(2, first.GetProperty("returned").GetInt32());
        Assert.Equal(2, first.GetProperty("next_offset").GetInt32());
        Assert.Equal(5, first.GetProperty("summary").GetProperty("objects").GetInt32());

        var last = JsonDocument.Parse(ObjectUsageReport.Build("Sales", 90, WindowStart, Now, [Side("srv", Tables(WindowStart, Now, tables))], null, 2, 4)).RootElement;
        Assert.Equal(1, last.GetProperty("returned").GetInt32());
        Assert.Equal(JsonValueKind.Null, last.GetProperty("next_offset").ValueKind);
    }

    [Fact]
    public void ABudgetCut_SetsNextOffsetToTheRowsThatFit_SoPagingSkipsNothing()
    {
        var tables = Enumerable.Range(0, 400).Select(i => Table("A_Rather_Long_Table_Name_For_The_Budget_" + i.ToString("D4", System.Globalization.CultureInfo.InvariantCulture))).ToArray();
        var json = ObjectUsageReport.Build("Sales", 90, WindowStart, Now, [Side("srv", Tables(WindowStart, Now, tables))], null, 400, 0);
        var root = JsonDocument.Parse(json).RootElement;

        var returned = root.GetProperty("returned").GetInt32();
        Assert.True(root.GetProperty("budget_truncated").GetBoolean());
        Assert.True(returned < 400);
        Assert.Equal(returned, root.GetProperty("objects").GetArrayLength());
        Assert.Equal(returned, root.GetProperty("next_offset").GetInt32());
        Assert.True(System.Text.Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes);
    }

    [Theory]
    [InlineData("dbo.usp_Run", "dbo", "usp_Run")]
    [InlineData("sales.usp.with.dots", "sales", "usp.with.dots")]
    [InlineData("NoSchema", "", "NoSchema")]
    public void ModuleNames_SplitAtTheFirstDot(string module, string schema, string name)
    {
        Assert.Equal((schema, name), ObjectUsageReport.SplitModuleName(module));
    }

    [Fact]
    public void ATableNoSnapshotMarks_IsNotACandidate_UnlessItsUseSettlesIt()
    {
        /* Right after the upgrade every stored row predates the memory-optimized flag. A memory-optimized table reads zero whatever its
           use, so an unmarked table with no use cannot be called unused; one with counted use is plainly not memory-optimized. */
        var root = Build(Side("srv", Tables(WindowStart, Now,
            Table("Unmarked", memoryOptimized: null),
            Table("UnmarkedButRead", reads: 4, memoryOptimized: null),
            Table("Marked"))));

        Assert.Equal(ObjectUsageReport.NotTracked, Obj(root, "Unmarked").GetProperty("classification").GetString());
        Assert.Equal(ObjectUsageReport.MemoryOptimizedUnknownCaveat, Obj(root, "Unmarked").GetProperty("caveat").GetString());
        Assert.Equal(ObjectUsageReport.Read, Obj(root, "UnmarkedButRead").GetProperty("classification").GetString());
        Assert.Equal(ObjectUsageReport.NotUsed, Obj(root, "Marked").GetProperty("classification").GetString());
    }

    [Fact]
    public void ATableOneSideMarks_IsSettledByThatSide()
    {
        var root = Build(
            Side("srv", TablesOn("NODE1", Table("Orders", memoryOptimized: null))),
            Side("srv:RO", TablesOn("NODE2", Table("Orders", memoryOptimized: false))));

        Assert.Equal(ObjectUsageReport.NotUsed, Obj(root, "Orders").GetProperty("classification").GetString());
    }

    [Fact]
    public void WithNoQueryStoreDataForTheDatabase_AModuleThatDidNotRun_IsNotACandidate()
    {
        var inventory = new ObjectInventory(Now, [new ObjectInventoryEntry("dbo", "usp_Hidden", "SQL_STORED_PROCEDURE", null, null)]);

        /* Query Store off, or the summary not built yet: nothing was looked at, so nothing can be said. */
        foreach (var executions in new[] { null, new ObjectExecutionWindow(null, null, "test", [], 0, QueryCaptureMode: "ALL") })
        {
            var root = Build(Side("srv", Tables(WindowStart, Now), executions, inventory));

            Assert.Equal(ObjectUsageReport.NotTracked, Obj(root, "usp_Hidden").GetProperty("classification").GetString());
            Assert.Equal(ObjectUsageReport.NoExecutionEvidenceCaveat, Obj(root, "usp_Hidden").GetProperty("caveat").GetString());
            Assert.Equal(0, root.GetProperty("summary").GetProperty("not_executed").GetInt32());
            Assert.Equal(1, root.GetProperty("summary").GetProperty("not_tracked").GetInt32());
        }
    }

    [Fact]
    public void WithQueryStoreDataForPartOfTheWindow_AModuleThatDidNotRun_SaysHowMuchWasCovered()
    {
        var inventory = new ObjectInventory(Now, [new ObjectInventoryEntry("dbo", "usp_YearEnd", "SQL_STORED_PROCEDURE", null, null)]);
        var partial = new ObjectExecutionWindow(Now.AddDays(-20), Now, "test", [], 20, QueryCaptureMode: "ALL");
        var whole = new ObjectExecutionWindow(WindowStart.AddDays(2), Now, "test", [], 88, QueryCaptureMode: "ALL");

        var shortRow = Obj(Build(Side("srv", Tables(WindowStart, Now), partial, inventory)), "usp_YearEnd");
        Assert.Equal(ObjectUsageReport.NotExecuted, shortRow.GetProperty("classification").GetString());
        Assert.StartsWith(ObjectUsageReport.PartialExecutionEvidenceCaveat(20, 90), shortRow.GetProperty("caveat").GetString(), StringComparison.Ordinal);

        /* The newest day or two are never in Darling's summary, so a window short by that much is whole. */
        var wholeRow = Obj(Build(Side("srv", Tables(WindowStart, Now), whole, inventory)), "usp_YearEnd");
        Assert.Equal(ObjectUsageReport.ProcedureCaveat, wholeRow.GetProperty("caveat").GetString());
    }

    [Fact]
    public void TheBestCoveredSide_DecidesWhatWasLookedAt()
    {
        /* The registrations read one Query Store: the read-only side having no summary rows does not make the primary's evidence partial. */
        var inventory = new ObjectInventory(Now, [new ObjectInventoryEntry("dbo", "fn_Multi", "SQL_TABLE_VALUED_FUNCTION", null, null)]);
        var root = Build(
            Side("srv", TablesOn("NODE1"), new ObjectExecutionWindow(WindowStart, Now, "test", [], 90, QueryCaptureMode: "ALL"), inventory),
            Side("srv:RO", TablesOn("NODE2"), new ObjectExecutionWindow(null, null, "test", [], 0, QueryCaptureMode: "ALL")));

        var row = Obj(root, "fn_Multi");
        Assert.Equal(ObjectUsageReport.NotExecuted, row.GetProperty("classification").GetString());
        Assert.Equal(JsonValueKind.Null, row.GetProperty("caveat").ValueKind);
    }

    [Fact]
    public void ANativelyCompiledModule_IsNotTracked_AndAProcedureThatDidNotRun_CarriesTheWrapperCaveat()
    {
        var inventory = new ObjectInventory(Now, [
            new ObjectInventoryEntry("dbo", "usp_Native", "SQL_STORED_PROCEDURE", null, null, IsNativelyCompiled: true),
            new ObjectInventoryEntry("dbo", "usp_NativeRan", "SQL_STORED_PROCEDURE", null, null, IsNativelyCompiled: true),
            new ObjectInventoryEntry("dbo", "usp_Wrapper", "SQL_STORED_PROCEDURE", null, null),
        ]);
        var executions = new ObjectExecutionWindow(WindowStart, Now, "test", [new ObjectExecutionTotals("dbo.usp_NativeRan", 7, Now)], 90, QueryCaptureMode: "ALL");

        var root = Build(Side("srv", Tables(WindowStart, Now), executions, inventory));

        Assert.Equal(ObjectUsageReport.NotTracked, Obj(root, "usp_Native").GetProperty("classification").GetString());
        Assert.Equal(ObjectUsageReport.NativelyCompiledCaveat, Obj(root, "usp_Native").GetProperty("caveat").GetString());
        /* With its statistics collection turned on, Query Store does record it, and then it ran. */
        Assert.Equal(ObjectUsageReport.Executed, Obj(root, "usp_NativeRan").GetProperty("classification").GetString());
        /* A procedure whose body only calls other modules shows no executions however often it runs, and the catalog cannot tell which. */
        Assert.Equal(ObjectUsageReport.NotExecuted, Obj(root, "usp_Wrapper").GetProperty("classification").GetString());
        Assert.Equal(ObjectUsageReport.ProcedureCaveat, Obj(root, "usp_Wrapper").GetProperty("caveat").GetString());
    }

    [Fact]
    public void TheTimeBeforeAReset_IsReported_AndLeftOutOfTheCoveredDays()
    {
        var reset = new ObjectUsageReset(Now.AddDays(-10), ObjectUsageResetReasons.Restart, "NODE1", "NODE1", Now.AddDays(-11));
        var arrival = new ObjectUsageReset(Now.AddDays(-5), ObjectUsageResetReasons.NodeChanged, "NODE1", "NODE2");
        var tables = new ObjectUsageWindowResult(
            WindowStart, Now, 91, TimeSpan.FromDays(1), [reset, arrival], ["NODE1", "NODE2"], [Table("Orders")], 0, TimeSpan.FromDays(1));

        var coverage = Build(Side("srv", tables)).GetProperty("coverage")[0].GetProperty("tables");

        Assert.Equal(24, coverage.GetProperty("blind_hours").GetDouble());
        Assert.Equal(89, coverage.GetProperty("covered_days").GetDouble());
        Assert.Equal(24, coverage.GetProperty("resets")[0].GetProperty("blind_hours").GetDouble());
        Assert.Equal(JsonValueKind.Null, coverage.GetProperty("resets")[1].GetProperty("blind_hours").ValueKind);
    }

    [Fact]
    public void TheServerPair_IsWrittenOnANodeChange_AndNotOnAPlainRestart()
    {
        // #8873: a restart whose previous snapshot had no server name (stored before the column existed) must not read like a failover.
        var restart = new ObjectUsageReset(Now.AddDays(-10), ObjectUsageResetReasons.Restart, null, "NODE1", Now.AddDays(-11));
        var arrival = new ObjectUsageReset(Now.AddDays(-5), ObjectUsageResetReasons.NodeChanged, "NODE1", "NODE2");
        var baseline = new ObjectUsageReset(Now.AddDays(-3), ObjectUsageResetReasons.NodeChanged | ObjectUsageResetReasons.NodeBaseline, "NODE2", "NODE3");
        var tables = new ObjectUsageWindowResult(
            WindowStart, Now, 91, TimeSpan.FromDays(1), [restart, arrival, baseline], ["NODE1", "NODE2", "NODE3"], [Table("Orders")]);

        var resets = Build(Side("srv", tables)).GetProperty("coverage")[0].GetProperty("tables").GetProperty("resets");

        Assert.False(resets[0].TryGetProperty("from_server", out _));
        Assert.False(resets[0].TryGetProperty("to_server", out _));
        Assert.Equal("NODE1", resets[1].GetProperty("from_server").GetString());
        Assert.Equal("NODE2", resets[1].GetProperty("to_server").GetString());
        Assert.Equal("NODE2", resets[2].GetProperty("from_server").GetString());
        Assert.Equal("NODE3", resets[2].GetProperty("to_server").GetString());
    }

    [Fact]
    public void PairedRows_CarryEachSidesNumbers()
    {
        var root = Build(
            Side("srv", TablesOn("NODE1", Table("Orders", reads: 3))),
            Side("srv:RO", TablesOn("NODE2", Table("Orders", reads: 4))));

        var byServer = Obj(root, "Orders").GetProperty("by_server");
        Assert.Equal(2, byServer.GetArrayLength());
        Assert.Equal(new long[] { 3, 4 }, byServer.EnumerateArray().Select(s => s.GetProperty("reads").GetInt64()).ToArray());
    }

    [Theory]
    [InlineData(null, 400, 400, null)]   /* the first tick: the configured value applies as it is */
    [InlineData(90, 400, 400, null)]     /* a longer retention applies at once */
    [InlineData(400, 400, 400, null)]
    [InlineData(400, 30, 400, 30)]       /* a shorter one starts waiting and deletes nothing */
    public void ARetentionChange_AppliesAtOnceWhenLonger_AndWaitsWhenShorter(int? applied, int configured, int expectedApplied, int? expectedPending)
    {
        var stored = applied is { } a ? new ObjectExecutionsDaily.RetentionState(a, null, null) : (ObjectExecutionsDaily.RetentionState?)null;

        var next = ObjectExecutionsDaily.NextRetention(stored, configured, Now);

        Assert.Equal(expectedApplied, next.AppliedDays);
        Assert.Equal(expectedPending, next.PendingDays);
        Assert.Equal(expectedPending is null ? null : Now, next.PendingSince);
    }

    [Fact]
    public void AShorterRetention_AppliesOnlyAfterItHasStoodForTheGracePeriod()
    {
        var grace = ObjectExecutionsDaily.RetentionLoweringGraceDays;
        var waiting = new ObjectExecutionsDaily.RetentionState(400, 30, Now);

        /* Still waiting the day before the grace period ends: the year of evidence is kept. */
        Assert.Equal(waiting, ObjectExecutionsDaily.NextRetention(waiting, 30, Now.AddDays(grace).AddMinutes(-1)));

        /* Put back in time: nothing was deleted and nothing is waiting. */
        Assert.Equal(new ObjectExecutionsDaily.RetentionState(400, null, null), ObjectExecutionsDaily.NextRetention(waiting, 400, Now.AddDays(1)));

        /* Changed to another shorter value: the wait starts again from the change. */
        Assert.Equal(
            new ObjectExecutionsDaily.RetentionState(400, 60, Now.AddDays(3)), ObjectExecutionsDaily.NextRetention(waiting, 60, Now.AddDays(3)));

        /* Stood for the whole period: it applies. */
        Assert.Equal(new ObjectExecutionsDaily.RetentionState(30, null, null), ObjectExecutionsDaily.NextRetention(waiting, 30, Now.AddDays(grace)));
    }

    [Theory]
    [InlineData(new[] { 1, 2 }, 1, 2, true)]    /* a scoped web reader whose grant holds the partner */
    [InlineData(new[] { 1 }, 1, 2, false)]      /* one whose grant does not */
    [InlineData(null, 1, 2, false)]             /* pinned to one server with no grant list in scope */
    [InlineData(null, null, 2, true)]           /* MCP, or an unscoped web reader */
    public void ThePairedRegistration_IsReadInsideTheGrant_NotByThePin(int[]? allowed, int? pinned, int partner, bool expected)
    {
        Assert.Equal(expected, DarlingServerResolver.PairedServerAllowed(partner, allowed, pinned));
    }

    [Fact]
    public void TheObjectList_RunsUnderTheUsageCountersDatabaseScope()
    {
        var overrides = new[]
        {
            new ScheduleOverride(123, "index_object_stats", null, null, null, ["Sales"]),
            new ScheduleOverride(123, "object_inventory", null, null, null, ["Other"]),
        };

        /* One scope for both, so no database has counters without an object list or a list without counters. */
        Assert.Equal(new[] { "Sales" }, StoreConfigProvider.ResolveDatabaseScope("index_object_stats", 123, overrides));
        Assert.Equal(new[] { "Sales" }, StoreConfigProvider.ResolveDatabaseScope("object_inventory", 123, overrides));
        Assert.Equal("query_store", DatabaseScopeFilter.ScopeOwner("query_store"));
    }

    [Theory]
    [InlineData("AUTO")]
    [InlineData("CUSTOM")]
    [InlineData("NONE")]
    [InlineData(" auto ")]
    public void UnderACaptureModeOtherThanAll_AModuleThatDidNotRun_IsNotTracked_AndTheCaveatNamesTheMode(string mode)
    {
        /* Review of #8855: AUTO (the default from SQL Server 2019) and CUSTOM skip rare, cheap statements, so a module that runs once a quarter
           can have no rows at all. Full coverage does not change that. */
        var inventory = new ObjectInventory(Now, [
            new ObjectInventoryEntry("dbo", "usp_Quarterly", "SQL_STORED_PROCEDURE", null, null),
            new ObjectInventoryEntry("dbo", "usp_Daily", "SQL_STORED_PROCEDURE", null, null),
        ]);
        var executions = new ObjectExecutionWindow(
            WindowStart, Now, "test", [new ObjectExecutionTotals("dbo.usp_Daily", 90, Now)], 90, QueryCaptureMode: mode);

        var root = Build(Side("srv", Tables(WindowStart, Now), executions, inventory));

        var quarterly = Obj(root, "usp_Quarterly");
        Assert.Equal(ObjectUsageReport.NotTracked, quarterly.GetProperty("classification").GetString());
        Assert.Equal(ObjectUsageReport.CaptureModeCaveat(mode.Trim().ToUpperInvariant()), quarterly.GetProperty("caveat").GetString());
        Assert.Contains(mode.Trim().ToUpperInvariant(), quarterly.GetProperty("caveat").GetString(), StringComparison.Ordinal);
        /* A module Query Store did see is still Executed. */
        Assert.Equal(ObjectUsageReport.Executed, Obj(root, "usp_Daily").GetProperty("classification").GetString());
        Assert.Equal(0, root.GetProperty("summary").GetProperty("not_executed").GetInt32());
        Assert.Equal(mode, root.GetProperty("coverage")[0].GetProperty("executions").GetProperty("query_capture_mode").GetString());
    }

    [Theory]
    [InlineData("ALL")]
    [InlineData(" all ")]
    public void UnderCaptureModeAll_AModuleThatDidNotRun_StaysACandidate(string mode)
    {
        var inventory = new ObjectInventory(Now, [new ObjectInventoryEntry("dbo", "fn_Multi", "SQL_TABLE_VALUED_FUNCTION", null, null)]);
        var executions = new ObjectExecutionWindow(WindowStart, Now, "test", [], 90, QueryCaptureMode: mode);

        var row = Obj(Build(Side("srv", Tables(WindowStart, Now), executions, inventory)), "fn_Multi");

        Assert.Equal(ObjectUsageReport.NotExecuted, row.GetProperty("classification").GetString());
        Assert.Equal(JsonValueKind.Null, row.GetProperty("caveat").ValueKind);
    }

    [Fact]
    public void WithNoStoredCaptureMode_AModuleThatDidNotRun_IsNotTracked_BecauseTheDefaultAutoLeavesRareStatementsOut()
    {
        /* #8784 review: no query_store_health row (the collector off, failing or scoped elsewhere) is not taken to be ALL. */
        var inventory = new ObjectInventory(Now, [new ObjectInventoryEntry("dbo", "fn_Multi", "SQL_TABLE_VALUED_FUNCTION", null, null)]);
        var executions = new ObjectExecutionWindow(WindowStart, Now, "test", [], 90, QueryCaptureMode: null);

        var row = Obj(Build(Side("srv", Tables(WindowStart, Now), executions, inventory)), "fn_Multi");

        Assert.Equal(ObjectUsageReport.NotTracked, row.GetProperty("classification").GetString());
        Assert.Equal(ObjectUsageReport.CaptureModeUnknownCaveat, row.GetProperty("caveat").GetString());
    }

    [Fact]
    public void AStoredAllOnOneSide_IsEnoughWhenTheOtherSideHasNoMode()
    {
        var inventory = new ObjectInventory(Now, [new ObjectInventoryEntry("dbo", "fn_Multi", "SQL_TABLE_VALUED_FUNCTION", null, null)]);
        var root = Build(
            Side("srv", TablesOn("NODE1"), new ObjectExecutionWindow(WindowStart, Now, "test", [], 90, QueryCaptureMode: null), inventory),
            Side("srv:RO", TablesOn("NODE2"), new ObjectExecutionWindow(WindowStart, Now, "test", [], 90, QueryCaptureMode: "ALL")));

        Assert.Equal(ObjectUsageReport.NotExecuted, Obj(root, "fn_Multi").GetProperty("classification").GetString());
    }

    [Fact]
    public void EffectiveCaptureMode_PrefersAPartialMode_ThenAll_ThenUnknown()
    {
        Assert.Equal("AUTO", ObjectUsageReport.EffectiveCaptureMode(["ALL", null, " auto "]));
        Assert.Equal("ALL", ObjectUsageReport.EffectiveCaptureMode([null, "all"]));
        Assert.Null(ObjectUsageReport.EffectiveCaptureMode([null, " "]));
        Assert.Null(ObjectUsageReport.EffectiveCaptureMode([]));
    }

    [Fact]
    public void ACaptureModeOtherThanAll_OnEitherPairedSide_StopsTheCandidate()
    {
        /* One Query Store, read twice: the side that saw AUTO is enough. */
        var inventory = new ObjectInventory(Now, [new ObjectInventoryEntry("dbo", "fn_Multi", "SQL_TABLE_VALUED_FUNCTION", null, null)]);
        var root = Build(
            Side("srv", TablesOn("NODE1"), new ObjectExecutionWindow(WindowStart, Now, "test", [], 90, QueryCaptureMode: "ALL"), inventory),
            Side("srv:RO", TablesOn("NODE2"), new ObjectExecutionWindow(WindowStart, Now, "test", [], 90, QueryCaptureMode: "AUTO")));

        var row = Obj(root, "fn_Multi");
        Assert.Equal(ObjectUsageReport.NotTracked, row.GetProperty("classification").GetString());
        Assert.Equal(ObjectUsageReport.CaptureModeCaveat("AUTO"), row.GetProperty("caveat").GetString());
    }

    [Fact]
    public void AHoleInTheQueryStoreData_CountsAgainstCoverage_EvenWhenTheSpanIsWhole()
    {
        /* Review of #8855: data near each end of the window and Query Store off in between. The span is the whole window, the days with data
           are 10, so a module that did not run says only 10 of the 90 days were looked at. */
        var inventory = new ObjectInventory(Now, [new ObjectInventoryEntry("dbo", "fn_Multi", "SQL_TABLE_VALUED_FUNCTION", null, null)]);
        var holed = new ObjectExecutionWindow(WindowStart, Now, "test", [], DaysWithData: 10, QueryCaptureMode: "ALL");

        var root = Build(Side("srv", Tables(WindowStart, Now), holed, inventory));

        var row = Obj(root, "fn_Multi");
        Assert.Equal(ObjectUsageReport.NotExecuted, row.GetProperty("classification").GetString());
        Assert.Equal(ObjectUsageReport.PartialExecutionEvidenceCaveat(10, 90), row.GetProperty("caveat").GetString());
        var coverage = root.GetProperty("coverage")[0].GetProperty("executions");
        Assert.True(coverage.GetProperty("short_of_window").GetBoolean());
        Assert.Equal(90, coverage.GetProperty("covered_days").GetDouble());
    }

    [Fact]
    public void TheEvidenceDays_AreTheDaysWithData_WhereKnown_NeverMoreThanTheSpan_AndTheSpanOtherwise()
    {
        Assert.Equal(10, ObjectUsageReport.ExecutionDays(new ObjectExecutionWindow(WindowStart, Now, "t", [], DaysWithData: 10, QueryCaptureMode: "ALL")));
        /* A count of days includes both ends, one more than the span: the span caps it. */
        Assert.Equal(20, ObjectUsageReport.ExecutionDays(new ObjectExecutionWindow(Now.AddDays(-20), Now, "t", [], DaysWithData: 21, QueryCaptureMode: "ALL")));
        Assert.Equal(90, ObjectUsageReport.ExecutionDays(new ObjectExecutionWindow(WindowStart, Now, "t", [], QueryCaptureMode: "ALL")));
        Assert.Equal(0, ObjectUsageReport.ExecutionDays(new ObjectExecutionWindow(null, null, "t", [], DaysWithData: 0, QueryCaptureMode: "ALL")));
        Assert.Equal(0, ObjectUsageReport.ExecutionDays(null));
    }

    [Fact]
    public void AModuleCreatedInsideTheWindow_ThatDidNotRun_SaysHowManyDaysItExisted()
    {
        /* Review of #8855: created yesterday, so "not executed in a quarter" would be a day's evidence presented as ninety. */
        var created = Now.AddDays(-1);
        var inventory = new ObjectInventory(Now, [
            new ObjectInventoryEntry("dbo", "fn_New", "SQL_TABLE_VALUED_FUNCTION", created, created),
            new ObjectInventoryEntry("dbo", "fn_Old", "SQL_TABLE_VALUED_FUNCTION", WindowStart.AddDays(-30), WindowStart.AddDays(-30)),
            new ObjectInventoryEntry("dbo", "usp_New", "SQL_STORED_PROCEDURE", Now.AddDays(-12.5), null),
        ]);
        var executions = new ObjectExecutionWindow(WindowStart, Now, "test", [], 90, QueryCaptureMode: "ALL");

        var root = Build(Side("srv", Tables(WindowStart, Now), executions, inventory));

        var fresh = Obj(root, "fn_New");
        Assert.Equal(ObjectUsageReport.NotExecuted, fresh.GetProperty("classification").GetString());
        Assert.Equal(ObjectUsageReport.CreatedInWindowCaveat(created, 1, 90), fresh.GetProperty("caveat").GetString());
        Assert.Contains("1 of the 90 days", fresh.GetProperty("caveat").GetString(), StringComparison.Ordinal);

        /* Created before the window: no such caveat. */
        Assert.Equal(JsonValueKind.Null, Obj(root, "fn_Old").GetProperty("caveat").ValueKind);

        /* It sits beside the procedure caveat, not in place of it. */
        var proc = Obj(root, "usp_New").GetProperty("caveat").GetString();
        Assert.StartsWith(ObjectUsageReport.CreatedInWindowCaveat(Now.AddDays(-12.5), 12.5, 90), proc, StringComparison.Ordinal);
        Assert.EndsWith(ObjectUsageReport.ProcedureCaveat, proc, StringComparison.Ordinal);
    }
}
