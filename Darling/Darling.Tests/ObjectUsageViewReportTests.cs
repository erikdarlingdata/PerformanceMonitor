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
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8784 part 2: views in the shared <see cref="ObjectUsageReport"/>. A view a Query Store statement names is <c>Referenced</c>; one the
/// dependency list reaches from something in use is too, with <c>used_via</c>; one nothing reaches is a candidate only where the evidence
/// could have shown it was used (statement text, capture mode ALL, a dependency list); names match ignoring case and under any schema when
/// written without one; and with no object list (Lite) only names statements referenced are listed. No database.
/// </summary>
public sealed class ObjectUsageViewReportTests
{
    private static readonly DateTime Now = new(2026, 11, 14, 12, 0, 0);
    private static readonly DateTime WindowStart = Now.AddDays(-90);

    private static ObjectUsageWindowResult Tables(params ObjectUsageTotals[] objects) =>
        new(WindowStart, Now, 91, TimeSpan.FromDays(1), [], ["NODE1"], objects);

    private static ObjectUsageTotals Table(string name, long reads = 0, bool indexedView = false, string schema = "dbo") =>
        new(name.GetHashCode(StringComparison.Ordinal), schema, name, indexedView, reads, 0, null, null, WindowStart, Now, 1, false);

    private static ObjectInventory Inventory(params (string Schema, string Name, string Type)[] objects) =>
        new(Now, objects.Select(o => new ObjectInventoryEntry(o.Schema, o.Name, o.Type, WindowStart.AddDays(-400), null)).ToList());

    private static ObjectReferenceWindow References(params ObjectReferenceTotals[] references) =>
        new(WindowStart, Now, 90, "test", references, Statements: 100);

    private static ObjectReferenceTotals Ref(string schema, string name, long executions = 1, bool textMatched = false, bool function = false) =>
        new(schema, name, executions, Now.AddDays(-1), textMatched, function);

    private static ObjectDependencyList Dependencies(params (string From, string To)[] edges) =>
        new(Now, edges.Select(e => new ObjectDependency("dbo", e.From, "V", "dbo", e.To, true)).ToList());

    private static ObjectExecutionWindow Executions(string? captureMode = "ALL", params ObjectExecutionTotals[] modules) =>
        new(WindowStart, Now, "test", modules, 90, captureMode);

    private static ObjectUsageSide Side(
        ObjectInventory? inventory, ObjectReferenceWindow? references, ObjectDependencyList? dependencies, ObjectExecutionWindow? executions = null,
        ObjectUsageWindowResult? tables = null, string server = "srv", params ObjectDependency[] inbound) =>
        new(server, tables ?? Tables(), t => t, executions ?? Executions(), inventory, references, dependencies, inbound.Length > 0 ? inbound : null);

    private static JsonElement Build(params ObjectUsageSide[] sides) =>
        JsonDocument.Parse(ObjectUsageReport.Build("Sales", 90, WindowStart, Now, sides, null, 100, 0)).RootElement;

    private static JsonElement Obj(JsonElement root, string name) =>
        root.GetProperty("objects").EnumerateArray().Single(o => o.GetProperty("object_name").GetString() == name);

    private static string? Class(JsonElement root, string name) => Obj(root, name).GetProperty("classification").GetString();

    [Fact]
    public void AViewAStatementNames_IsReferenced_WithTheStatementsExecutions()
    {
        var root = Build(Side(Inventory(("dbo", "vOrders", "VIEW")), References(Ref("dbo", "vOrders", executions: 42)), Dependencies()));

        var row = Obj(root, "vOrders");
        Assert.Equal(ObjectUsageReport.Referenced, row.GetProperty("classification").GetString());
        Assert.Equal("VIEW", row.GetProperty("object_type").GetString());
        Assert.Equal(42, row.GetProperty("referencing_executions").GetInt64());
        Assert.NotEqual(JsonValueKind.Null, row.GetProperty("last_referenced_utc").ValueKind);
        Assert.Equal(JsonValueKind.Null, row.GetProperty("executions").ValueKind);
        Assert.Equal(1, root.GetProperty("summary").GetProperty("referenced").GetInt32());
    }

    [Fact]
    public void AViewNothingReaches_IsACandidate_WhenTheEvidenceCouldHaveShownIt()
    {
        var root = Build(Side(
            Inventory(("dbo", "vUsed", "VIEW"), ("dbo", "vOld", "VIEW")), References(Ref("dbo", "vUsed")), Dependencies()));

        var row = Obj(root, "vOld");
        Assert.Equal(ObjectUsageReport.NotUsed, row.GetProperty("classification").GetString());
        Assert.Equal(0, row.GetProperty("referencing_executions").GetInt64());
        Assert.Contains(ObjectUsageReport.ViewCaveat, row.GetProperty("caveat").GetString(), StringComparison.Ordinal);

        /* Candidates first. */
        Assert.Equal("vOld", root.GetProperty("objects")[0].GetProperty("object_name").GetString());
    }

    [Fact]
    public void WithNoDependencyList_AViewNoStatementNamed_IsNotTracked()
    {
        var root = Build(Side(Inventory(("dbo", "vOld", "VIEW")), References(Ref("dbo", "other")), dependencies: null));

        Assert.Equal(ObjectUsageReport.NotTracked, Class(root, "vOld"));
        Assert.Equal(ObjectUsageReport.NoDependencyListCaveat, Obj(root, "vOld").GetProperty("caveat").GetString());
    }

    [Fact]
    public void WithNoStatementText_AViewNoStatementNamed_IsNotTracked()
    {
        var noText = new ObjectReferenceWindow(null, null, 0, "test", []);
        var root = Build(Side(Inventory(("dbo", "vOld", "VIEW")), noText, Dependencies()));

        Assert.Equal(ObjectUsageReport.NotTracked, Class(root, "vOld"));
        Assert.Equal(ObjectUsageReport.NoReferenceEvidenceCaveat, Obj(root, "vOld").GetProperty("caveat").GetString());
    }

    [Fact]
    public void UnderACaptureModeOtherThanAll_AViewNoStatementNamed_IsNotTracked()
    {
        var root = Build(Side(Inventory(("dbo", "vOld", "VIEW")), References(), Dependencies(), Executions("AUTO")));

        Assert.Equal(ObjectUsageReport.NotTracked, Class(root, "vOld"));
        Assert.Contains("AUTO", Obj(root, "vOld").GetProperty("caveat").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public void ShortStatementText_AndStatementsWithoutText_AreInTheCaveat()
    {
        var shortText = new ObjectReferenceWindow(Now.AddDays(-20), Now, 20, "test", [], Statements: 50, StatementsWithoutText: 7);
        var root = Build(Side(Inventory(("dbo", "vOld", "VIEW")), shortText, Dependencies()));

        var caveat = Obj(root, "vOld").GetProperty("caveat").GetString()!;
        Assert.Equal(ObjectUsageReport.NotUsed, Class(root, "vOld"));
        Assert.Contains("covers only 20 of the 90 days", caveat, StringComparison.Ordinal);
        Assert.Contains("7 of the 50 statements", caveat, StringComparison.Ordinal);
    }

    [Fact]
    public void AViewUsedOnlyThroughAnotherView_IsReferenced_ThroughTheChain()
    {
        var root = Build(Side(
            Inventory(("dbo", "vOuter", "VIEW"), ("dbo", "vInner", "VIEW"), ("dbo", "vDeep", "VIEW"), ("dbo", "vLonely", "VIEW")),
            References(Ref("dbo", "vOuter")),
            Dependencies(("vOuter", "vInner"), ("vInner", "vDeep"), ("vLonely", "vDeep"))));

        Assert.Equal(ObjectUsageReport.Referenced, Class(root, "vInner"));
        Assert.Equal("dbo.vOuter", Obj(root, "vInner").GetProperty("used_via").GetString());
        Assert.Equal(ObjectUsageReport.Referenced, Class(root, "vDeep"));
        Assert.Equal("dbo.vInner", Obj(root, "vDeep").GetProperty("used_via").GetString());

        /* An edge FROM an unused view is not evidence: the walk only goes forward from what is in use. */
        Assert.Equal(ObjectUsageReport.NotUsed, Class(root, "vLonely"));
    }

    [Fact]
    public void AViewAndAProcedureReachedFromAModuleThatRan_AreReferenced()
    {
        var root = Build(Side(
            Inventory(("dbo", "usp_Wrapper", "SQL_STORED_PROCEDURE"), ("dbo", "usp_Inner", "SQL_STORED_PROCEDURE"), ("dbo", "vReport", "VIEW")),
            References(),
            Dependencies(("usp_Wrapper", "usp_Inner"), ("usp_Inner", "vReport")),
            Executions("ALL", new ObjectExecutionTotals("dbo.usp_Wrapper", 12, Now.AddDays(-2)))));

        Assert.Equal(ObjectUsageReport.Executed, Class(root, "usp_Wrapper"));
        Assert.Equal(ObjectUsageReport.Referenced, Class(root, "usp_Inner"));
        Assert.Equal("dbo.usp_Wrapper", Obj(root, "usp_Inner").GetProperty("used_via").GetString());
        Assert.Equal(ObjectUsageReport.Referenced, Class(root, "vReport"));
        Assert.Equal("dbo.usp_Inner", Obj(root, "vReport").GetProperty("used_via").GetString());
    }

    [Fact]
    public void AnUnqualifiedName_MatchesTheViewUnderAnySchema_IgnoringCase()
    {
        var root = Build(Side(
            Inventory(("rpt", "vSales", "VIEW"), ("dbo", "vSales", "VIEW"), ("rpt", "vOther", "VIEW")),
            References(Ref("", "VSALES")),
            Dependencies()));

        Assert.Equal(ObjectUsageReport.Referenced, root.GetProperty("objects").EnumerateArray()
            .Where(o => o.GetProperty("object_name").GetString() == "vSales")
            .Select(o => o.GetProperty("classification").GetString()).Distinct().Single());
        Assert.Equal(ObjectUsageReport.NotUsed, Class(root, "vOther"));
    }

    [Fact]
    public void ASchemaQualifiedName_MatchesThatSchemaOnly()
    {
        var root = Build(Side(Inventory(("rpt", "vSales", "VIEW"), ("dbo", "vSales", "VIEW")), References(Ref("rpt", "vSales")), Dependencies()));

        var rows = root.GetProperty("objects").EnumerateArray().Where(o => o.GetProperty("object_name").GetString() == "vSales").ToList();
        Assert.Equal(ObjectUsageReport.Referenced, rows.Single(o => o.GetProperty("schema_name").GetString() == "rpt").GetProperty("classification").GetString());
        Assert.Equal(ObjectUsageReport.NotUsed, rows.Single(o => o.GetProperty("schema_name").GetString() == "dbo").GetProperty("classification").GetString());
    }

    [Fact]
    public void AnIndexedView_IsListedOnce_AndAStatementNamingItLiftsAnUnusedCount()
    {
        var root = Build(Side(
            Inventory(("dbo", "vIndexed", "VIEW")),
            References(Ref("dbo", "vIndexed", executions: 3)),
            Dependencies(),
            tables: Tables(Table("vIndexed", indexedView: true))));

        var row = Assert.Single(root.GetProperty("objects").EnumerateArray(), o => o.GetProperty("object_name").GetString() == "vIndexed");
        Assert.Equal("INDEXED_VIEW", row.GetProperty("object_type").GetString());
        Assert.Equal(ObjectUsageReport.Referenced, row.GetProperty("classification").GetString());
        Assert.Equal(3, row.GetProperty("referencing_executions").GetInt64());
    }

    [Fact]
    public void ATableAStatementNames_KeepsItsCountedClassification()
    {
        var root = Build(Side(Inventory(), References(Ref("dbo", "Orders")), Dependencies(), tables: Tables(Table("Orders"))));

        Assert.Equal(ObjectUsageReport.NotUsed, Class(root, "Orders"));
        Assert.Equal(JsonValueKind.Null, Obj(root, "Orders").GetProperty("referencing_executions").ValueKind);
    }

    [Fact]
    public void AnInlineFunctionAStatementNames_IsReferenced_NotUntracked()
    {
        var root = Build(Side(
            Inventory(("dbo", "fnOrders", "SQL_INLINE_TABLE_VALUED_FUNCTION"), ("dbo", "fnUnseen", "SQL_INLINE_TABLE_VALUED_FUNCTION")),
            References(Ref("dbo", "fnOrders", function: true)),
            Dependencies()));

        Assert.Equal(ObjectUsageReport.Referenced, Class(root, "fnOrders"));
        Assert.Equal(ObjectUsageReport.NotTracked, Class(root, "fnUnseen"));
    }

    [Fact]
    public void AViewFoundOnlyByTheTextSearch_SaysSo()
    {
        var root = Build(Side(Inventory(("dbo", "vOrders", "VIEW")), References(Ref("", "vOrders", textMatched: true)), Dependencies()));

        Assert.Equal(ObjectUsageReport.Referenced, Class(root, "vOrders"));
        Assert.Equal(ObjectUsageReport.TextMatchedCaveat, Obj(root, "vOrders").GetProperty("caveat").GetString());
    }

    [Fact]
    public void TwoSides_ReadOneQueryStore_SoAViewsCountIsTheLargerNotTheSum()
    {
        var inventory = Inventory(("dbo", "vOrders", "VIEW"));
        var root = Build(
            Side(inventory, References(Ref("dbo", "vOrders", executions: 10)), Dependencies(), server: "srv"),
            Side(inventory, References(Ref("dbo", "vOrders", executions: 7)), Dependencies(), server: "srv:RO"));

        var row = Obj(root, "vOrders");
        Assert.Equal(10, row.GetProperty("referencing_executions").GetInt64());
        Assert.Equal(2, row.GetProperty("by_server").GetArrayLength());
    }

    [Fact]
    public void WithNoObjectList_OnlyNamesStatementsReferenced_AreListed_AsGuessedViews()
    {
        var root = Build(Side(
            inventory: null,
            References(Ref("dbo", "vReport", executions: 5), Ref("dbo", "Orders"), Ref("dbo", "fnThing", function: true), Ref("", "sysobjects"),
                Ref("", "vMaybe", textMatched: true)),
            dependencies: null,
            tables: Tables(Table("Orders", reads: 9))));

        var report = Obj(root, "vReport");
        Assert.Equal("VIEW", report.GetProperty("object_type").GetString());
        Assert.Equal(ObjectUsageReport.Referenced, report.GetProperty("classification").GetString());
        Assert.Equal(ObjectUsageReport.GuessedViewCaveat, report.GetProperty("caveat").GetString());
        Assert.Equal(5, report.GetProperty("referencing_executions").GetInt64());

        var names = root.GetProperty("objects").EnumerateArray().Select(o => o.GetProperty("object_name").GetString()).ToArray();
        Assert.Equal(new[] { "Orders", "vReport" }, names.OrderBy(n => n, StringComparer.Ordinal).ToArray());
        Assert.Contains("a view no statement named is NOT listed", root.GetProperty("note").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public void TheCoverage_SaysHowMuchStatementTextWasLookedAt()
    {
        var refs = new ObjectReferenceWindow(WindowStart, Now, 88, "the source", [], Statements: 120, StatementsWithoutText: 4, StatementsUnparsed: 2);
        var root = Build(Side(Inventory(), refs, Dependencies()));

        var views = root.GetProperty("coverage")[0].GetProperty("views");
        Assert.Equal(88, views.GetProperty("days_with_data").GetInt32());
        Assert.Equal(120, views.GetProperty("statements").GetInt64());
        Assert.Equal(4, views.GetProperty("statements_without_text").GetInt64());
        Assert.Equal(2, views.GetProperty("statements_unparsed").GetInt64());
        Assert.Equal("the source", views.GetProperty("source").GetString());
        Assert.NotEqual(JsonValueKind.Null, root.GetProperty("coverage")[0].GetProperty("dependency_list_captured_at_utc").ValueKind);
    }

    [Fact]
    public void WithNoReferenceRead_TheNoteStillSaysViewsAreNotCovered()
    {
        var root = Build(Side(Inventory(("dbo", "vOld", "VIEW")), references: null, dependencies: null));

        Assert.Contains("Views that are not indexed are not covered", root.GetProperty("note").GetString(), StringComparison.Ordinal);
        Assert.Equal(ObjectUsageReport.NotTracked, Class(root, "vOld"));
    }

    // ── #8784 review ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void WithNoStoredCaptureMode_AViewNothingReaches_IsNotTracked()
    {
        var root = Build(Side(Inventory(("dbo", "vOld", "VIEW")), References(Ref("dbo", "other")), Dependencies(), Executions(captureMode: null)));

        Assert.Equal(ObjectUsageReport.NotTracked, Class(root, "vOld"));
        Assert.Equal(ObjectUsageReport.CaptureModeUnknownCaveat, Obj(root, "vOld").GetProperty("caveat").GetString());
    }

    [Fact]
    public void AViewAnotherDatabasesViewReads_IsReferenced_AndTheWalkGoesOnFromIt()
    {
        /* Reports.dbo.vReport selects from Sales.dbo.vCustomer, which reads vAddress; nothing in Sales names either. */
        var inbound = new ObjectDependency("dbo", "vReport", "V", "dbo", "vCustomer", false, ReferencingDatabase: "Reports");
        var root = Build(Side(
            Inventory(("dbo", "vCustomer", "VIEW"), ("dbo", "vAddress", "VIEW"), ("dbo", "vOld", "VIEW")),
            References(Ref("dbo", "other")),
            Dependencies(("vCustomer", "vAddress")),
            inbound: inbound));

        Assert.Equal(ObjectUsageReport.Referenced, Class(root, "vCustomer"));
        Assert.Equal("Reports.dbo.vReport", Obj(root, "vCustomer").GetProperty("used_via").GetString());
        Assert.Equal(ObjectUsageReport.Referenced, Class(root, "vAddress"));
        Assert.Equal("dbo.vCustomer", Obj(root, "vAddress").GetProperty("used_via").GetString());
        Assert.Equal(ObjectUsageReport.NotUsed, Class(root, "vOld"));
    }

    [Fact]
    public void InboundEdgesAlone_WithNoOwnDependencyList_StillLiftTheirTargets()
    {
        var inbound = new ObjectDependency("dbo", "vReport", "V", "", "vCustomer", false, ReferencingDatabase: "Reports");
        var root = Build(Side(Inventory(("dbo", "vCustomer", "VIEW")), References(), dependencies: null, inbound: inbound));

        Assert.Equal(ObjectUsageReport.Referenced, Class(root, "vCustomer"));
    }

    [Fact]
    public void AViewUsedThroughASynonym_IsReferenced()
    {
        var synonym = new ObjectDependencyList(Now, [new ObjectDependency("dbo", "synOrders", "SN", "dbo", "vOrders", false)]);
        var root = Build(Side(Inventory(("dbo", "vOrders", "VIEW")), References(Ref("dbo", "synOrders")), synonym));

        Assert.Equal(ObjectUsageReport.Referenced, Class(root, "vOrders"));
        Assert.Equal("dbo.synOrders", Obj(root, "vOrders").GetProperty("used_via").GetString());
    }

    [Fact]
    public void AViewUsedOnlyInsideAScalarFunction_IsReferenced_ThroughTheCallOfTheFunction()
    {
        var root = Build(Side(
            Inventory(("dbo", "GetRate", "SQL_SCALAR_FUNCTION"), ("dbo", "vRates", "VIEW")),
            References(Ref("dbo", "GetRate", function: true)),
            new ObjectDependencyList(Now, [new ObjectDependency("dbo", "GetRate", "FN", "dbo", "vRates", true)])));

        Assert.Equal(ObjectUsageReport.Referenced, Class(root, "vRates"));
        Assert.Equal("dbo.GetRate", Obj(root, "vRates").GetProperty("used_via").GetString());
        Assert.Equal(ObjectUsageReport.Referenced, Class(root, "GetRate"));
    }

    [Theory]
    [InlineData("", "sysobjects", true)]
    [InlineData("", "SYSINDEXES", true)]
    [InlineData("", "SysUserSettings", false)]
    [InlineData("", "systemConfig", false)]
    [InlineData("dbo", "sysobjects", false)]
    public void TheCatalogNamesLeftOut_AreTheCompatibilityViews_NotEveryNameStartingSys(string schema, string name, bool catalog) =>
        Assert.Equal(catalog, ObjectUsageReport.IsCatalogName(schema, name));

    [Fact]
    public void WithNoObjectList_AViewNamedLikeASystemTable_IsStillListed()
    {
        var root = Build(Side(null, References(Ref("", "SysUserSettings"), Ref("", "sysobjects")), dependencies: null));

        Assert.Equal(ObjectUsageReport.Referenced, Class(root, "SysUserSettings"));
        Assert.DoesNotContain(root.GetProperty("objects").EnumerateArray(), o => o.GetProperty("object_name").GetString() == "sysobjects");
    }
}
