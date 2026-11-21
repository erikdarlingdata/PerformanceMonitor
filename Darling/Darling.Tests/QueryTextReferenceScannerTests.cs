/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Analysis.ObjectReferences;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8784 part 2: the shared pieces the view usage read is built on. <see cref="QueryTextReferenceScanner"/> (names a statement reads as
/// table sources, its local names left out, Query Store's parameter list stripped), <see cref="SqlWholeWordMatcher"/> (the fallback for a
/// statement that does not parse), <see cref="ObjectReferenceAggregator"/> (per-name totals per statement database) and the scan cache. No
/// database.
/// </summary>
public sealed class QueryTextReferenceScannerTests
{
    private static string[] Names(string sql) =>
        QueryTextReferenceScanner.Scan(sql).References
            .Select(r => string.Join(".", new[] { r.Server, r.Database, r.Schema, r.Name }.Where(p => p is not null)))
            .ToArray();

    [Fact]
    public void ASelect_NamesItsTablesAndViews_AsWritten()
    {
        var scan = QueryTextReferenceScanner.Scan("SELECT o.id FROM dbo.vOrders AS o JOIN Sales.dbo.Customers c ON c.id = o.cid JOIN Products p ON 1 = 1");

        Assert.True(scan.Parsed);
        Assert.Equal(new[] { "dbo.vOrders", "Sales.dbo.Customers", "Products" }, Names("SELECT o.id FROM dbo.vOrders AS o JOIN Sales.dbo.Customers c ON c.id = o.cid JOIN Products p ON 1 = 1"));
        Assert.All(scan.References, r => Assert.Equal(ScannedReferenceKind.TableOrView, r.Kind));
    }

    [Fact]
    public void NamesInCommentsAndStrings_DoNotCount()
    {
        var names = Names("/* FROM dbo.vOld */ SELECT 'FROM dbo.vOlder' AS x -- dbo.vOldest\r\nFROM dbo.vNew");

        Assert.Equal(new[] { "dbo.vNew" }, names);
    }

    [Fact]
    public void WriteTargets_AreNamed()
    {
        Assert.Equal(new[] { "dbo.vTarget" }, Names("UPDATE dbo.vTarget SET a = 1 WHERE b = 2"));
        Assert.Equal(new[] { "dbo.vTarget", "dbo.Source" }, Names("INSERT INTO dbo.vTarget (a) SELECT a FROM dbo.Source"));
        Assert.Equal(new[] { "dbo.vTarget" }, Names("DELETE FROM dbo.vTarget WHERE a = 1"));
        Assert.Equal(new[] { "dbo.vTarget", "dbo.Source" }, Names("MERGE dbo.vTarget AS t USING dbo.Source AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET a = s.a;"));
    }

    [Fact]
    public void ACte_IsNotAViewOfTheSameName()
    {
        var names = Names("WITH vOrders AS (SELECT id FROM dbo.Orders) SELECT id FROM vOrders");

        Assert.Equal(new[] { "dbo.Orders" }, names);
    }

    [Fact]
    public void ASchemaQualifiedName_IsNeverTakenForACte()
    {
        var names = Names("WITH vOrders AS (SELECT 1 AS id) SELECT id FROM dbo.vOrders");

        Assert.Equal(new[] { "dbo.vOrders" }, names);
    }

    [Fact]
    public void TheAliasTargetOfAnUpdate_IsNotAView()
    {
        var names = Names("UPDATE o SET o.a = 1 FROM dbo.Orders AS o WHERE o.id = 2");

        Assert.Equal(new[] { "dbo.Orders" }, names);
    }

    [Fact]
    public void AReferenceThatCarriesItsOwnAlias_IsKept_EvenWhenTheAliasIsItsName()
    {
        var names = Names("SELECT 1 FROM vOrders AS vOrders");

        Assert.Equal(new[] { "vOrders" }, names);
    }

    [Fact]
    public void TempTablesAndTableVariables_AreDropped()
    {
        var names = Names("SELECT a FROM #work AS w JOIN @rows AS r ON r.a = w.a JOIN ##global AS g ON 1 = 1 JOIN dbo.Real AS x ON 1 = 1");

        Assert.Equal(new[] { "dbo.Real" }, names);
    }

    [Fact]
    public void ATableValuedFunction_IsNamedAsAFunction()
    {
        var scan = QueryTextReferenceScanner.Scan("SELECT * FROM dbo.fnOrders(1) AS f CROSS APPLY dbo.fnLines(f.id) AS l");

        Assert.True(scan.Parsed);
        Assert.Equal(new[] { "fnOrders", "fnLines" }, scan.References.Select(r => r.Name).ToArray());
        Assert.All(scan.References, r => Assert.Equal(ScannedReferenceKind.Function, r.Kind));
    }

    [Fact]
    public void ADerivedTableAlias_IsNotAView()
    {
        var names = Names("SELECT d.a FROM (SELECT a FROM dbo.T) AS d JOIN d2 ON 1 = 1 CROSS JOIN (VALUES (1)) AS d2(x)");

        Assert.Equal(new[] { "dbo.T" }, names);
    }

    [Fact]
    public void QueryStoresParameterList_IsStrippedBeforeTheParse()
    {
        const string text = "(@p1 int,@p2 nvarchar(50),@p3 decimal(10,2),@p4 varchar(max))SELECT a FROM dbo.vOrders WHERE id = @p1";

        Assert.Equal("SELECT a FROM dbo.vOrders WHERE id = @p1", QueryTextReferenceScanner.StripParameterList(text));
        Assert.Equal(new[] { "dbo.vOrders" }, Names(text));
    }

    [Theory]
    [InlineData("(SELECT 1) UNION SELECT 2")]
    [InlineData("SELECT 1")]
    [InlineData("  (@p1 int")]
    public void AStatementWithNoParameterList_IsLeftAlone(string text)
    {
        Assert.Equal(text, QueryTextReferenceScanner.StripParameterList(text));
    }

    [Theory]
    [InlineData("IF EXISTS (SELECT 1 FROM dbo.vFlags WHERE a = 1)")]
    [InlineData("(@x int)IF (SELECT COUNT(*) FROM dbo.vFlags) > @x")]
    [InlineData("WHILE EXISTS (SELECT 1 FROM dbo.vFlags)")]
    [InlineData("if not exists(select * from dbo.vFlags)")]
    public void ABareConditionQueryStoreKeptOnItsOwn_ParsesAndNamesItsTables(string text)
    {
        var scan = QueryTextReferenceScanner.Scan(text);

        Assert.True(scan.Parsed);
        Assert.Equal("vFlags", Assert.Single(scan.References).Name);
    }

    [Theory]
    [InlineData("IF", true)]
    [InlineData("  if(1=1)", true)]
    [InlineData("WHILE 1=1", true)]
    [InlineData("IFFY", false)]
    [InlineData("SELECT 1", false)]
    [InlineData("IF_x = 1", false)]
    public void ABareCondition_IsAnIfOrWhileKeyword(string text, bool expected)
    {
        Assert.Equal(expected, QueryTextReferenceScanner.IsBareCondition(text));
    }

    [Fact]
    public void AParameterListWithAQuotedParenthesis_IsStrippedAtItsRealEnd()
    {
        Assert.Equal("SELECT 1", QueryTextReferenceScanner.StripParameterList("(@p1 nvarchar(10) = N')')SELECT 1"));
    }

    [Fact]
    public void ADoubleQuotedString_ParsesUnderQuotedIdentifierOff()
    {
        var scan = QueryTextReferenceScanner.Scan("SELECT a FROM dbo.vOrders WHERE b = \"it's a string\"");

        Assert.True(scan.Parsed);
        Assert.Equal(new[] { "dbo.vOrders" }, Names("SELECT a FROM dbo.vOrders WHERE b = \"it's a string\""));
    }

    [Theory]
    [InlineData("SELEC a FROM dbo.vOrders")]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public void AStatementThatDoesNotParse_ComesBackUnparsed(string? text)
    {
        var scan = QueryTextReferenceScanner.Scan(text);

        Assert.False(scan.Parsed);
        Assert.Empty(scan.References);
    }

    [Fact]
    public void AStatementPastTheParseLimit_ComesBackUnparsed()
    {
        var text = "SELECT a FROM dbo.vOrders /*" + new string('x', QueryTextReferenceScanner.MaxParseLength) + "*/";

        Assert.False(QueryTextReferenceScanner.Scan(text).Parsed);
    }

    [Fact]
    public void ANameWrittenTwice_IsNamedOnce_IgnoringCase()
    {
        Assert.Equal(new[] { "dbo.vOrders" }, Names("SELECT 1 FROM dbo.vOrders AS a JOIN DBO.VORDERS AS b ON 1 = 1"));
    }

    [Theory]
    [InlineData("SELECT * FROM dbo.person", true)]
    [InlineData("SELECT * FROM [person]", true)]
    [InlineData("SELECT * FROM person WHERE 1 = 1", true)]
    [InlineData("SELECT person_id FROM t", false)]
    [InlineData("SELECT @person", false)]
    [InlineData("SELECT * FROM #person", false)]
    [InlineData("SELECT * FROM PERSON", true)]
    [InlineData("SELECT * FROM person$", false)]
    public void TheWholeWordMatch_UsesTSqlIdentifierCharacters(string text, bool expected)
    {
        Assert.Equal(expected, SqlWholeWordMatcher.Contains(text, "person"));
    }

    [Fact]
    public void TheWholeWordMatch_FindsALaterMatchAfterAnEarlierPartialOne()
    {
        Assert.Equal(17, SqlWholeWordMatcher.IndexOf("SELECT person_id person", "person", 0));
    }

    [Fact]
    public void TheAggregator_SumsExecutionsPerName_AndClearsTheStatementsOwnDatabase()
    {
        var aggregator = new ObjectReferenceAggregator();
        var t1 = new DateTime(2026, 11, 1, 10, 0, 0);
        var t2 = new DateTime(2026, 11, 2, 10, 0, 0);
        aggregator.Add("Sales", "SELECT 1 FROM dbo.vOrders", 5, t1);
        aggregator.Add("Sales", "SELECT 1 FROM Sales.dbo.vOrders JOIN Reports.dbo.vTotals ON 1 = 1", 3, t2);
        aggregator.Add("Sales", "SELECT 1 FROM sys.objects JOIN INFORMATION_SCHEMA.TABLES ON 1 = 1 JOIN LinkSrv.Db.dbo.Remote ON 1 = 1", 7, t2);

        var orders = Assert.Single(aggregator.References, r => r.ObjectName == "vOrders");
        Assert.Equal("", orders.ReferencedDatabase);
        Assert.Equal(8, orders.Executions);
        Assert.Equal(2, orders.Statements);
        Assert.Equal(t2, orders.LastExecution);

        var totals = Assert.Single(aggregator.References, r => r.ObjectName == "vTotals");
        Assert.Equal("Reports", totals.ReferencedDatabase);

        /* sys, INFORMATION_SCHEMA and a linked server are never a monitored database's view. */
        Assert.Equal(2, aggregator.References.Count);

        var database = Assert.Single(aggregator.Databases);
        Assert.Equal(3, database.Statements);
        Assert.Equal(15, database.Executions);
    }

    [Fact]
    public void TheAggregator_SearchesAnUnparsedStatement_ForTheFallbackNames_AsWholeWords()
    {
        var aggregator = new ObjectReferenceAggregator(db => db == "Sales" ? ["vOrders", "vOrd", "vMissing"] : []);
        aggregator.Add("Sales", "SELEC * FROM dbo.vOrders", 4, null);
        aggregator.Add("Sales", null, 9, null);
        aggregator.Add("Other", "SELEC * FROM dbo.vOrders", 2, null);

        var hit = Assert.Single(aggregator.References);
        Assert.Equal("vOrders", hit.ObjectName);
        Assert.Equal("", hit.SchemaName);
        Assert.True(hit.TextMatched);
        Assert.Equal(4, hit.Executions);

        var sales = Assert.Single(aggregator.Databases, d => d.DatabaseName == "Sales");
        Assert.Equal(2, sales.Statements);
        Assert.Equal(1, sales.StatementsWithoutText);
        Assert.Equal(1, sales.StatementsUnparsed);
        Assert.Equal(1, Assert.Single(aggregator.Databases, d => d.DatabaseName == "Other").StatementsUnparsed);
    }

    [Fact]
    public void TheScanCache_ParsesEachTextOnce_AndStartsAgainWhenFull()
    {
        var cache = new QueryTextScanCache(capacity: 2);
        var first = cache.GetOrScan("SELECT 1 FROM dbo.A");
        Assert.Same(first, cache.GetOrScan("SELECT 1 FROM dbo.A"));
        cache.GetOrScan("SELECT 1 FROM dbo.B");
        Assert.Equal(2, cache.Count);

        cache.GetOrScan("SELECT 1 FROM dbo.C");
        Assert.Equal(1, cache.Count);
    }
}
