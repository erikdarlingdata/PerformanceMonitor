/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8994: the inline query_stats capture (Azure SQL Database, or <c>queryStatsDeferredPlanFetch</c> off) stores one plan per
/// statement shape, as the deferred fetch does. Each run drives the step the runner takes before the write
/// (<see cref="DarlingCollectorRunner.LinkInlineQueryStatsPlansByShape(int, IEnumerable{QueryStatsCollector.Row}, PlanDigestCache{QueryStatsPlanShapeKey}, DateTime)"/>)
/// and the settle after the commit, and counts the distinct plans the batch would store. The placeholder tests read their
/// rows through the collector's own inline read, so the filter's marker comes from the real statement filter.
/// </summary>
public sealed class QueryStatsInlinePlanShapeTests
{
    private const int ServerId = 11;
    private static readonly DateTime Start = new(2026, 11, 18, 9, 0, 0, DateTimeKind.Utc);

    private int _nextHandle;

    private sealed class Store
    {
        public readonly PlanDigestCache<QueryStatsPlanShapeKey> Shapes = new();
        public readonly HashSet<string> Digests = new(StringComparer.Ordinal);
    }

    private static string QueryHash(int shape) => "0xA" + shape.ToString("X15", CultureInfo.InvariantCulture);

    private static string PlanHash(int shape) => "0xB" + shape.ToString("X15", CultureInfo.InvariantCulture);

    /// <summary>A plan whose StatementText carries the statement's own literal, as an ad hoc plan's XML does.</summary>
    private static string PlanWith(string statementText) =>
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\"><BatchSequence><Batch><Statements>"
        + "<StmtSimple StatementText=\"" + statementText + "\" /></Statements></Batch></BatchSequence></ShowPlanXML>";

    /// <summary>One inline row with its plan rendered: a fresh plan_handle, and a plan carrying this execution's literal.</summary>
    private QueryStatsCollector.Row AdHoc(int shape, string? planHash = null, string database = "app")
    {
        var handle = ++_nextHandle;
        var plan = PlanWith("SELECT * FROM dbo.t" + shape.ToString(CultureInfo.InvariantCulture)
            + " WHERE id = " + handle.ToString(CultureInfo.InvariantCulture));
        return new QueryStatsCollector.Row
        {
            PlanHandle = "0x06" + handle.ToString("X8", CultureInfo.InvariantCulture),
            DatabaseName = database,
            StatementEndOffset = -1,
            QueryHash = QueryHash(shape),
            QueryPlanHash = planHash ?? PlanHash(shape),
            QueryPlanXml = plan,
            QueryPlanXmlBytes = plan.Length * 2L,
        };
    }

    /// <summary>A procedure statement: the same plan_handle and the same plan on every run.</summary>
    private static QueryStatsCollector.Row Procedure(int n)
    {
        var plan = PlanWith("SELECT * FROM dbo.p" + n.ToString(CultureInfo.InvariantCulture) + " WHERE id = @id");
        return new QueryStatsCollector.Row
        {
            PlanHandle = "0x05" + n.ToString("X8", CultureInfo.InvariantCulture),
            DatabaseName = "app",
            StatementEndOffset = -1,
            QueryHash = "0xC" + n.ToString("X15", CultureInfo.InvariantCulture),
            QueryPlanHash = "0xD" + n.ToString("X15", CultureInfo.InvariantCulture),
            QueryPlanXml = plan,
            QueryPlanXmlBytes = plan.Length * 2L,
        };
    }

    /// <summary>One run: the link step, what the batch stores (one dimension row per distinct plan text, nothing for a
    /// known digest), and the settle. Returns how many plans the store did not hold before.</summary>
    private static int Run(Store store, List<QueryStatsCollector.Row> rows, DateTime now, bool committed = true)
    {
        var pending = DarlingCollectorRunner.LinkInlineQueryStatsPlansByShape(ServerId, rows, store.Shapes, now);

        var added = 0;
        if (committed)
        {
            foreach (var row in rows.Where(r => r.QueryPlanXml is not null))
            {
                if (store.Digests.Add(ProcedureStatsPlanReuse.DigestOf(row.QueryPlanXml!)))
                {
                    added++;
                }
            }
        }

        store.Shapes.Settle(pending, committed, now);
        return added;
    }

    private List<QueryStatsCollector.Row> Workload(int shapes, int variantsPerShape, int procedures)
    {
        var rows = new List<QueryStatsCollector.Row>();
        for (var v = 0; v < variantsPerShape; v++)
        {
            for (var s = 0; s < shapes; s++)
            {
                rows.Add(AdHoc(s));
            }
        }

        rows.AddRange(Enumerable.Range(0, procedures).Select(Procedure));
        return rows;
    }

    [Fact]
    public void TenInlineRunsOfLiteralOnlyVariants_StoreOnePlanPerShape_AndEveryRowKeepsAPlan()
    {
        var store = new Store();
        var added = new List<int>();

        for (var run = 0; run < 10; run++)
        {
            /* 5 ad hoc shapes run 60 times each with new literals, plus 20 procedures, every plan rendered inline. */
            var rows = Workload(shapes: 5, variantsPerShape: 60, procedures: 20);
            added.Add(Run(store, rows, Start.AddMinutes(run)));

            Assert.All(rows, r => Assert.True(r.QueryPlanXml is not null || r.KnownPlanDigest is not null));
        }

        Assert.Equal(25, added[0]);
        Assert.All(added.Skip(1), n => Assert.Equal(0, n));
        Assert.Equal(25, store.Digests.Count);
    }

    [Fact]
    public void ANewPlanShape_ForAStatementAlreadyStored_IsStillStored()
    {
        var store = new Store();
        Run(store, Workload(shapes: 1, variantsPerShape: 10, procedures: 0), Start);

        var recompiled = AdHoc(0, planHash: "0xBEEF000000000001");
        var sameShape = AdHoc(0);
        var added = Run(store, new List<QueryStatsCollector.Row> { recompiled, sameShape }, Start.AddMinutes(1));

        Assert.Equal(1, added);
        Assert.NotNull(recompiled.QueryPlanXml);
        Assert.Null(recompiled.KnownPlanDigest);
        Assert.Null(sameShape.QueryPlanXml);
        Assert.NotNull(sameShape.KnownPlanDigest);
        Assert.Contains(sameShape.KnownPlanDigest, store.Digests);
        Assert.Equal(2, store.Digests.Count);
    }

    [Fact]
    public void TheSameHashesInTwoDatabases_AreTwoShapes()
    {
        var store = new Store();
        var rows = new List<QueryStatsCollector.Row> { AdHoc(0, database: "tenant1"), AdHoc(0, database: "tenant2") };

        Assert.Equal(2, Run(store, rows, Start));
    }

    [Fact]
    public void ABatchThatFailed_LeavesTheShapeUnknown_SoTheNextRunStoresIt()
    {
        var store = new Store();
        Run(store, new List<QueryStatsCollector.Row> { AdHoc(0) }, Start, committed: false);

        var next = AdHoc(0);
        Assert.Equal(1, Run(store, new List<QueryStatsCollector.Row> { next }, Start.AddMinutes(1)));
        Assert.Null(next.KnownPlanDigest);
    }

    [Fact]
    public void AShapeWhoseStoredPlanAgedOut_IsStoredAgainOnItsNextSighting()
    {
        var store = new Store();
        var first = AdHoc(0);
        Run(store, new List<QueryStatsCollector.Row> { first }, Start);

        /* The store reported the digest absent (its dimension row aged out): the runner evicts it. */
        store.Shapes.Evict(store.Digests.ToList());
        store.Digests.Clear();

        var next = AdHoc(0);
        Assert.Equal(1, Run(store, new List<QueryStatsCollector.Row> { next }, Start.AddMinutes(1)));
        Assert.Null(next.KnownPlanDigest);
    }

    [Fact]
    public void ARowWithNoPlanXml_IsLeftAlone_EvenWhenItsShapeIsKnown()
    {
        var store = new Store();
        Run(store, new List<QueryStatsCollector.Row> { AdHoc(0) }, Start);

        /* Outside the inline slots (no plan, no size) and over the size cap (a size, no plan). */
        var outside = AdHoc(0);
        outside.QueryPlanXml = null;
        outside.QueryPlanXmlBytes = null;
        var overCap = AdHoc(0);
        overCap.QueryPlanXml = null;
        overCap.QueryPlanXmlBytes = 9_000_000;
        Run(store, new List<QueryStatsCollector.Row> { outside, overCap }, Start.AddMinutes(1));

        Assert.Null(outside.KnownPlanDigest);
        Assert.Null(outside.QueryPlanXmlBytes);
        Assert.Null(overCap.KnownPlanDigest);
        Assert.Equal(9_000_000, overCap.QueryPlanXmlBytes);
    }

    [Fact]
    public void RowsWithNoUsableShape_StoreTheirOwnPlans_AsBefore()
    {
        var store = new Store();
        var rows = Enumerable.Range(0, 3).Select(_ => AdHoc(0, planHash: "0x0000000000000000")).ToList();

        Assert.Equal(3, Run(store, rows, Start));
        Assert.All(rows, r => Assert.Null(r.KnownPlanDigest));
    }

    // ---- the collector's report: a placeholder is never a shape's plan ------------------------------------------------

    private sealed class NoDeltas : ICollectorDeltaCalculator
    {
        public long CalculateDelta(int serverId, string collectorName, string key, long currentValue,
            DateTime? collectionTime = null, int maxGapSeconds = 0) => currentValue;

        public long CalculateDeltaWithInterval(int serverId, string collectorName, string key, long currentValue,
            out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0)
        {
            intervalSeconds = 0;
            return currentValue;
        }
    }

    private static CollectorContext InlineContext(Func<SensitiveStatements.Session>? scrub, bool capture = true) => new()
    {
        ServerId = ServerId,
        ServerName = "test-server",
        CollectionTime = Start,
        Deltas = new NoDeltas(),
        Target = new CollectorTargetInfo(),
        CapturePlanXml = capture,
        ScrubSessionFactory = scrub,
    };

    /// <summary>A session whose 15-second budget is already spent: its fake judge charges 20 seconds to a fake clock.</summary>
    private static SensitiveStatements.Session SpentSession()
    {
        var now = TimeSpan.Zero;
        var session = new SensitiveStatements.Session(
            _ =>
            {
                now += TimeSpan.FromSeconds(20);
                return SensitiveStatements.Verdict.Clean;
            },
            () => now,
            TimeSpan.FromSeconds(15),
            new SensitiveStatements.TimedOutMemo());
        session.Text("a value that spends the budget");
        return session;
    }

    /// <summary>A plan the filter cannot parse (it is cut off) that holds an auto-parameter token and a parameter value: a
    /// probe would have been made had it parsed, so the filter withholds it whole without asking the judge. This is the
    /// marker's second trigger, with the read's budget NOT spent.</summary>
    private static string UnparseableAutoParameterPlan(string statementText) =>
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\"><BatchSequence><Batch><Statements>"
        + "<StmtSimple StatementText=\"" + statementText + " WHERE id = @1\"><QueryPlan><ParameterList>"
        + "<ColumnReference Column=\"@1\" ParameterCompiledValue=\"(1)\" /></ParameterList>";

    /// <summary>Reads one statement of <paramref name="shape"/> through the collector's inline read (the main query's 46
    /// columns), with <paramref name="plan"/> in the plan column.</summary>
    private async Task<QueryStatsCollector.Row> ReadInlineAsync(CollectorContext context, int shape, string plan)
    {
        using var table = new DataTable();
        for (var i = 0; i < 44; i++)
        {
            table.Columns.Add("c" + i.ToString(CultureInfo.InvariantCulture), i switch
            {
                3 or 4 => typeof(DateTime),
                0 or 1 or 2 or 36 or 37 or 38 or 42 => typeof(string),
                40 or 41 or 43 => typeof(int),
                _ => typeof(long),
            });
        }

        if (context.CapturePlanXml)
        {
            table.Columns.Add("query_plan_xml", typeof(string));
            table.Columns.Add("query_plan_xml_bytes", typeof(long));
        }

        var values = new object[table.Columns.Count];
        Array.Fill(values, DBNull.Value);
        var handle = ++_nextHandle;
        values[0] = "app";
        values[1] = QueryHash(shape);
        values[2] = PlanHash(shape);
        values[36] = "0x02" + handle.ToString("X8", CultureInfo.InvariantCulture);
        values[37] = "0x06" + handle.ToString("X8", CultureInfo.InvariantCulture);
        values[40] = 0;
        values[41] = -1;
        if (context.CapturePlanXml)
        {
            values[44] = plan;
            values[45] = plan.Length * 2L;
        }

        table.Rows.Add(values);
        await using var reader = table.CreateDataReader();
        return Assert.Single(await QueryStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None));
    }

    [Fact]
    public async Task APlaceholderPlan_IsNeverCachedAsTheShapesPlan_AndTheNextRealSightingStoresTheRealPlan()
    {
        var store = new Store();
        var withheld = await ReadInlineAsync(InlineContext(SpentSession), 0, PlanWith("SELECT * FROM dbo.t0 WHERE id = 1"));

        Assert.True(withheld.QueryPlanXmlIsPlaceholder);
        Assert.NotNull(withheld.QueryPlanXml);
        Run(store, new List<QueryStatsCollector.Row> { withheld }, Start);
        Assert.Null(withheld.KnownPlanDigest);

        /* The next sighting, read with a fresh budget, is the real plan: it is stored, not linked to the marker. */
        var real = await ReadInlineAsync(InlineContext(null), 0, PlanWith("SELECT * FROM dbo.t0 WHERE id = 2"));
        Assert.False(real.QueryPlanXmlIsPlaceholder);
        var realDigest = ProcedureStatsPlanReuse.DigestOf(real.QueryPlanXml!);
        Run(store, new List<QueryStatsCollector.Row> { real }, Start.AddMinutes(1));
        Assert.Null(real.KnownPlanDigest);
        Assert.NotNull(real.QueryPlanXml);

        /* And the shape's plan from then on is that real plan. */
        var later = await ReadInlineAsync(InlineContext(null), 0, PlanWith("SELECT * FROM dbo.t0 WHERE id = 3"));
        Run(store, new List<QueryStatsCollector.Row> { later }, Start.AddMinutes(2));
        Assert.Equal(realDigest, later.KnownPlanDigest);
        Assert.Null(later.QueryPlanXml);
    }

    [Fact]
    public async Task APlanTheFilterWithheldForItsOwnSake_IsNeverCachedAsTheShapesPlan_AndTheNextCleanSightingStoresTheRealPlan()
    {
        var store = new Store();
        var withheld = await ReadInlineAsync(InlineContext(null), 0, UnparseableAutoParameterPlan("SELECT * FROM dbo.t0"));

        Assert.True(withheld.QueryPlanXmlIsPlaceholder);
        Assert.True(SensitiveStatements.IsWithheldPlan(withheld.QueryPlanXml));
        Run(store, new List<QueryStatsCollector.Row> { withheld }, Start);
        Assert.Null(withheld.KnownPlanDigest);

        var real = await ReadInlineAsync(InlineContext(null), 0, PlanWith("SELECT * FROM dbo.t0 WHERE id = 2"));
        Assert.False(real.QueryPlanXmlIsPlaceholder);
        var realDigest = ProcedureStatsPlanReuse.DigestOf(real.QueryPlanXml!);
        Run(store, new List<QueryStatsCollector.Row> { real }, Start.AddMinutes(1));
        Assert.Null(real.KnownPlanDigest);
        Assert.NotNull(real.QueryPlanXml);

        var later = await ReadInlineAsync(InlineContext(null), 0, PlanWith("SELECT * FROM dbo.t0 WHERE id = 3"));
        Run(store, new List<QueryStatsCollector.Row> { later }, Start.AddMinutes(2));
        Assert.Equal(realDigest, later.KnownPlanDigest);
        Assert.Null(later.QueryPlanXml);
    }

    [Fact]
    public async Task APlaceholderRowOfAKnownShape_LinksToTheShapesRealPlan()
    {
        var store = new Store();
        var real = await ReadInlineAsync(InlineContext(null), 0, PlanWith("SELECT * FROM dbo.t0 WHERE id = 1"));
        var realDigest = ProcedureStatsPlanReuse.DigestOf(real.QueryPlanXml!);
        Run(store, new List<QueryStatsCollector.Row> { real }, Start);

        var withheld = await ReadInlineAsync(InlineContext(SpentSession), 0, PlanWith("SELECT * FROM dbo.t0 WHERE id = 2"));
        Assert.True(withheld.QueryPlanXmlIsPlaceholder);
        Run(store, new List<QueryStatsCollector.Row> { withheld }, Start.AddMinutes(1));

        Assert.Equal(realDigest, withheld.KnownPlanDigest);
        Assert.Null(withheld.QueryPlanXml);
    }

    [Fact]
    public async Task AReadThatCapturesNoPlans_ReportsNoPlaceholder()
    {
        /* Lite's read: CapturePlanXml off, so the main query has no plan columns and no row carries a plan. */
        var row = await ReadInlineAsync(InlineContext(SpentSession, capture: false), 0, PlanWith("SELECT 1"));

        Assert.Null(row.QueryPlanXml);
        Assert.Null(row.QueryPlanXmlBytes);
        Assert.False(row.QueryPlanXmlIsPlaceholder);
    }
}
