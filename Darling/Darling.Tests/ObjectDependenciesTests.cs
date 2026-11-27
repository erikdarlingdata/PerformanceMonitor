/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8784 part 2: the shape of <see cref="ObjectDependenciesCollector"/> (the Darling-only daily dependency list) and the rows
/// <see cref="ObjectDependencyFilter"/> keeps and drops. No database.
/// </summary>
public sealed class ObjectDependenciesTests
{
    private static RawObjectDependency Row(
        string referencedName, string? schema = "dbo", string? database = null, string? server = null, string referencingType = "V",
        bool bound = true, bool callerDependent = false, bool ambiguous = false) =>
        new("dbo", "vOuter", referencingType, server, database, schema, referencedName, bound, callerDependent, ambiguous);

    [Fact]
    public void TheCollector_IsDarlingOnly_APlainTable_Daily_InTheCatalog_UnderTheIndexStatsScope()
    {
        var collector = ObjectDependenciesCollector.Instance;
        Assert.True(collector.IsDarlingOnly);
        Assert.True(collector.StaysPlainTable);
        Assert.Contains(collector, CollectorCatalog.All);
        Assert.Equal(1440, CollectorScheduleDefaults.All["object_dependencies"].FrequencyMinutes);
        Assert.Equal("index_object_stats", DatabaseScopeFilter.ScopeOwner("object_dependencies"));
        Assert.Equal("index_object_stats", DatabaseScopeFilter.ScopeOwner("OBJECT_DEPENDENCIES"));
    }

    [Fact]
    public void TheCollector_ReadsUserObjectReferences_WithTheBoundNameWhereThereIsOne()
    {
        var body = ObjectDependenciesCollector.BuildPayloadBody();
        Assert.Contains("FROM sys.sql_expression_dependencies AS d", body, StringComparison.Ordinal);
        Assert.Contains("o.is_ms_shipped = 0", body, StringComparison.Ordinal);
        Assert.Contains("d.referencing_class = 1", body, StringComparison.Ordinal);
        Assert.Contains("d.referenced_class = 1", body, StringComparison.Ordinal);
        Assert.Contains("referenced_schema = COALESCE(rs.name, d.referenced_schema_name)", body, StringComparison.Ordinal);
        Assert.Contains("referenced_name = COALESCE(ro.name, d.referenced_entity_name)", body, StringComparison.Ordinal);
        Assert.Contains("SELECT DISTINCT", body, StringComparison.Ordinal);
        Assert.Contains("OPTION(RECOMPILE)", body, StringComparison.Ordinal);
    }

    [Fact]
    public void WithoutSelectOnTheCatalogView_TheDatabaseShipsNoRows_InsteadOfFailing()
    {
        /* Only db_owner has SELECT on sys.sql_expression_dependencies by default; without the check every database would fail with Msg 229. */
        var body = ObjectDependenciesCollector.BuildPayloadBody();
        var guard = body.IndexOf("HAS_PERMS_BY_NAME(N'sys.sql_expression_dependencies', N'OBJECT', N'SELECT')", StringComparison.Ordinal);
        Assert.True(guard >= 0);
        Assert.True(guard < body.IndexOf("FROM sys.sql_expression_dependencies", StringComparison.Ordinal));
        Assert.Contains("SELECT TOP (0)", body, StringComparison.Ordinal);
        Assert.Contains("RETURN;", body, StringComparison.Ordinal);
    }

    [Fact]
    public void ThePayloadColumns_FollowTheSelectList()
    {
        var names = ObjectDependenciesCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();
        Assert.Equal(
            new[]
            {
                "database_name", "referencing_schema", "referencing_name", "referencing_type", "referenced_server", "referenced_database",
                "referenced_schema", "referenced_name", "is_bound", "is_caller_dependent", "is_ambiguous",
            },
            names);

        /* Every column but database_name is read by ordinal from the SELECT list, in this order. */
        var body = ObjectDependenciesCollector.BuildPayloadBody();
        var last = -1;
        foreach (var name in names.Skip(1))
        {
            var at = body.IndexOf("    " + name + " = ", StringComparison.Ordinal);
            Assert.True(at > last, $"{name} is out of order in the SELECT list.");
            last = at;
        }
    }

    [Fact]
    public void ThePerItemQuery_QuoteDoublesTheBody_AndEscapesTheDatabaseName()
    {
        var query = ObjectDependenciesCollector.Instance.BuildPerItemQuery("odd]name", null!);
        Assert.Contains("EXECUTE [odd]]name].sys.sp_executesql", query.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void ALocalReference_IsKept_WithItsBoundSchema()
    {
        var edge = ObjectDependencyFilter.ToEdge(Row("vInner"), "Sales");

        Assert.NotNull(edge);
        Assert.Equal(("dbo", "vOuter", "V", "dbo", "vInner", true),
            (edge!.ReferencingSchema, edge.ReferencingName, edge.ReferencingType, edge.ReferencedSchema, edge.ReferencedName, edge.IsBound));
    }

    [Fact]
    public void AReferenceToTheSameDatabaseByName_IsKept_IgnoringCase()
    {
        Assert.NotNull(ObjectDependencyFilter.ToEdge(Row("vInner", database: "SALES"), "Sales"));
    }

    [Theory]
    [InlineData("vInner", "dbo", "Reports", null)]
    [InlineData("vInner", "dbo", "Sales", "LinkSrv")]
    [InlineData("objects", "sys", null, null)]
    [InlineData("TABLES", "INFORMATION_SCHEMA", null, null)]
    [InlineData("value", "col", "alias", null)]
    [InlineData("nodes", null, "x", null)]
    public void RowsThatNeverNameAUserObjectOfTheDatabase_AreDropped(string name, string? schema, string? database, string? server)
    {
        Assert.Null(ObjectDependencyFilter.ToEdge(Row(name, schema, database, server), "Sales"));
    }

    [Theory]
    [InlineData("inserted")]
    [InlineData("DELETED")]
    public void TheTriggerPseudoTables_AreDropped_InATriggerOnly(string name)
    {
        Assert.Null(ObjectDependencyFilter.ToEdge(Row(name, schema: null, referencingType: "TR", bound: false), "Sales"));
        Assert.NotNull(ObjectDependencyFilter.ToEdge(Row(name, schema: null, referencingType: "P", bound: false), "Sales"));
    }

    [Fact]
    public void CallerDependentAndAmbiguousRows_AreKept_ByName()
    {
        /* A kept row can only make a view look used; a dropped one could make a used view look unused. */
        var edge = ObjectDependencyFilter.ToEdge(Row("vInner", schema: null, bound: false, callerDependent: true, ambiguous: true), "Sales");

        Assert.NotNull(edge);
        Assert.Equal("", edge!.ReferencedSchema);
        Assert.False(edge.IsBound);
    }

    [Fact]
    public void ToEdges_DropsWhatToEdgeDrops()
    {
        var edges = ObjectDependencyFilter.ToEdges([Row("vInner"), Row("objects", "sys"), Row("vOther", database: "Reports")], "Sales");

        Assert.Equal("vInner", Assert.Single(edges).ReferencedName);
    }

    // ── #8784 review ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void TheCollector_ShipsEachSynonym_AsARowOfItsOwn_ItsBaseObjectSplitByParsename()
    {
        /* Synonyms are never the referencing object in sys.sql_expression_dependencies, so a statement naming one would reach nothing. */
        var body = ObjectDependenciesCollector.BuildPayloadBody();
        Assert.Contains("FROM sys.synonyms AS sn", body, StringComparison.Ordinal);
        Assert.Contains("referencing_type = CONVERT(nvarchar(2), N'SN')", body, StringComparison.Ordinal);
        foreach (var (column, part) in new[] { ("referenced_server", 4), ("referenced_database", 3), ("referenced_schema", 2), ("referenced_name", 1) })
        {
            Assert.Contains($"{column} = PARSENAME(sn.base_object_name, {part})", body, StringComparison.Ordinal);
        }

        /* Inside the permission check: a database that ships synonyms but no dependency list would read as having a list. */
        Assert.True(body.IndexOf("RETURN;", StringComparison.Ordinal) < body.IndexOf("sys.synonyms", StringComparison.Ordinal));
        Assert.True(body.IndexOf("UNION ALL", StringComparison.Ordinal) > body.IndexOf("sys.sql_expression_dependencies AS d", StringComparison.Ordinal));
    }

    [Fact]
    public void ASynonymRow_IsAnEdgeLikeAnyOther()
    {
        var edge = ObjectDependencyFilter.ToEdge(
            new RawObjectDependency("dbo", "synOrders", "SN", null, null, "dbo", "vOrders", false, false, false), "Sales");

        Assert.Equal(("synOrders", "SN", "vOrders"), (edge!.ReferencingName, edge.ReferencingType, edge.ReferencedName));
    }

    [Fact]
    public void ARowOfAnotherDatabase_NamingThisOneByThreeParts_IsAnInboundEdge_IgnoringCase()
    {
        var edge = ObjectDependencyFilter.ToInboundEdge(Row("vCustomer", database: "SALES"), "Reports", "Sales");

        Assert.NotNull(edge);
        Assert.Equal(("Reports", "vOuter", "vCustomer"), (edge!.ReferencingDatabase, edge.ReferencingName, edge.ReferencedName));
        Assert.Null(ObjectDependencyFilter.ToEdge(Row("vCustomer", database: "SALES"), "Reports"));
    }

    [Theory]
    [InlineData("vCustomer", "dbo", null, null)]           // no database part: the other database's own object
    [InlineData("vCustomer", "dbo", "Archive", null)]      // a third database
    [InlineData("vCustomer", "dbo", "Sales", "LINKED")]   // a linked server is never this server's database
    [InlineData("value", "col", "Sales", null)]           // an XML type method artifact
    [InlineData("objects", "sys", "Sales", null)]
    [InlineData("TABLES", "INFORMATION_SCHEMA", "Sales", null)]
    public void RowsThatDoNotNameAUserObjectOfThisDatabase_AreNotInboundEdges(string name, string? schema, string? database, string? server) =>
        Assert.Null(ObjectDependencyFilter.ToInboundEdge(Row(name, schema, database, server), "Reports", "Sales"));

    [Fact]
    public void ADatabasesOwnRows_AreNeverItsInboundEdges() =>
        Assert.Null(ObjectDependencyFilter.ToInboundEdge(Row("vCustomer", database: "Sales"), "sales", "Sales"));
}
