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
    public void AScalarFunctionCall_IsNamedAsAFunction_WithItsDatabaseWhereWritten()
    {
        /* #8784 review: an inlined scalar UDF gets no executions in Query Store, so the call is where the walk to a view it reads starts. */
        var scan = QueryTextReferenceScanner.Scan("SELECT dbo.GetRate(t.id), Rates.dbo.GetTax(t.id) FROM dbo.Orders AS t WHERE dbo.IsOpen(t.id) = 1");

        Assert.True(scan.Parsed);
        var functions = scan.References.Where(r => r.Kind == ScannedReferenceKind.Function)
            .Select(r => string.Join(".", new[] { r.Database, r.Schema, r.Name }.Where(p => p is not null))).Order(StringComparer.Ordinal).ToArray();
        Assert.Equal(new[] { "Rates.dbo.GetTax", "dbo.GetRate", "dbo.IsOpen" }, functions);
    }

    [Fact]
    public void ABuiltInFunction_AndAnXmlTypeMethod_AreNotNamed()
    {
        var scan = QueryTextReferenceScanner.Scan(
            "SELECT COUNT(*), ISNULL(t.a, 0), x.value('(/r)[1]', 'int'), t.doc.exist('/r') FROM dbo.T AS t CROSS APPLY t.doc.nodes('/r') AS n(x)");

        Assert.True(scan.Parsed);
        Assert.DoesNotContain(scan.References, r => r.Kind == ScannedReferenceKind.Function);
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

        /* The text split into words once gives the same answer as the search, for a name made of word characters (#8784 review). */
        Assert.True(SqlWholeWordMatcher.IsWord("person"));
        Assert.Equal(expected, SqlWholeWordMatcher.Words(text).Contains("person"));
    }

    [Theory]
    [InlineData("person", true)]
    [InlineData("v_Order$2", true)]
    [InlineData("my view", false)]
    [InlineData("a.b", false)]
    [InlineData("", false)]
    public void AWordName_IsMadeOnlyOfIdentifierCharacters(string name, bool expected)
    {
        Assert.Equal(expected, SqlWholeWordMatcher.IsWord(name));
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
    public void ASameViewNameInTwoSchemasOrCases_IsCountedOncePerStatement_UnderItsFirstSpelling()
    {
        var calls = 0;
        var aggregator = new ObjectReferenceAggregator(_ => { calls++; return ["vOrders", "VORDERS", "vOrders"]; });
        aggregator.Add("Sales", "SELEC * FROM dbo.vOrders", 4, null);
        aggregator.Add("Sales", "SELEC * FROM dbo.vorders", 6, null);

        var hit = Assert.Single(aggregator.References);
        Assert.Equal("vOrders", hit.ObjectName);
        Assert.Equal(2, hit.Statements);
        Assert.Equal(10, hit.Executions);
        Assert.Equal(1, calls);
    }

    [Fact]
    public void AnUnparsedStatement_IsSplitIntoWordsOnce_AndTheCacheKeepsThem()
    {
        /* #8784 review: the text search ran once per view over the whole text, with nothing kept. The cache now keeps an unparsed statement's
           words with its scan, so a later day looks each view up in them; a name with a space is still searched for in the text. */
        var cache = new QueryTextScanCache();
        const string text = "SELEC * FROM dbo.vOrders JOIN [my view] ON 1 = 1 WHERE vOrd_id = 1";
        var scan = cache.GetOrScan(text);
        Assert.False(scan.Parsed);
        Assert.NotNull(scan.Words);
        Assert.Same(scan, cache.GetOrScan(text));
        Assert.Contains("VORDERS", scan.Words!);
        Assert.Null(QueryTextReferenceScanner.Scan("SELECT 1 FROM dbo.t").Words);

        foreach (var withCache in new[] { true, false })
        {
            var aggregator = new ObjectReferenceAggregator(_ => ["vOrders", "vOrd", "my view", "vMissing"], withCache ? cache : null);
            aggregator.Add("Sales", text, 3, null);
            Assert.Equal(["my view", "vOrders"], aggregator.References.Select(r => r.ObjectName).Order(StringComparer.Ordinal));
        }
    }

    [Fact]
    public void TheAggregator_MergesAndKeysNamesByOneFold_SoTwoRowsCanNeverShareAStoredKey()
    {
        /* #8784 review: merging by one case rule and keying the stored row by another (PostgreSQL's lower()) could land two rows on one key
           and fail the upsert. The aggregator now merges on the fold it hands out as the key, and the build stores that key as it is, so
           two rows always have two keys: "vİtem" (U+0130) and "vItem" fold apart and stay two rows with two keys, while "DBO.VITEM",
           "dbo.vItem" and "SALES.dbo.vItem" run in Sales fold together into one. */
        var aggregator = new ObjectReferenceAggregator();
        aggregator.Add("Sales", "SELECT 1 FROM dbo.vİtem", 2, null);
        aggregator.Add("Sales", "SELECT 1 FROM DBO.VITEM", 3, null);
        aggregator.Add("Sales", "SELECT 1 FROM SALES.dbo.vItem", 4, null);

        var rows = aggregator.References;
        Assert.Equal(rows.Count, rows.Select(r => (r.DatabaseName, r.ReferencedDatabaseKey, r.SchemaKey, r.ObjectNameKey, r.IsFunction)).Distinct().Count());
        Assert.All(rows, r => Assert.Equal(ObjectReferenceAggregator.FoldName(r.ObjectName), r.ObjectNameKey));

        /* Whether U+0130 folds to "i" is the runtime's call (ICU keeps it); either way the merge follows the fold, never another rule. */
        var dottedFoldsTogether = ObjectReferenceAggregator.FoldName("vİtem") == "vitem";
        Assert.Equal(dottedFoldsTogether ? 1 : 2, rows.Count);
        var plain = Assert.Single(rows, r => r.ObjectNameKey == "vitem");
        Assert.Equal(dottedFoldsTogether ? 9 : 7, plain.Executions);
        Assert.Equal(("", "dbo"), (plain.ReferencedDatabaseKey, plain.SchemaKey));
    }

    [Fact]
    public void TheScanCache_ParsesEachTextOnce_AndWhenFullDropsOnlyTheScansNotUsedForAGeneration()
    {
        var cache = new QueryTextScanCache(capacity: 2);
        var a = cache.GetOrScan("SELECT 1 FROM dbo.A");
        Assert.Same(a, cache.GetOrScan("SELECT 1 FROM dbo.A"));
        var b = cache.GetOrScan("SELECT 1 FROM dbo.B");
        Assert.Equal(2, cache.Count);

        /* The current generation (A, B) is full: C starts a new one, and A and B are still held. */
        cache.GetOrScan("SELECT 1 FROM dbo.C");
        Assert.Equal(3, cache.Count);

        /* A hit in the previous generation moves it up, so it outlives B when the generation turns over again. */
        Assert.Same(a, cache.GetOrScan("SELECT 1 FROM dbo.A"));
        cache.GetOrScan("SELECT 1 FROM dbo.D");
        Assert.Same(a, cache.GetOrScan("SELECT 1 FROM dbo.A"));
        Assert.NotSame(b, cache.GetOrScan("SELECT 1 FROM dbo.B"));
    }
}
