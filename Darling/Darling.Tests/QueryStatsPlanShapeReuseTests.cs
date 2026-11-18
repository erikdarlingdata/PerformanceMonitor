/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8994: the deferred query_stats plan fetch stores one plan per statement shape (database, query_hash, query_plan_hash),
/// not one per literal-only variant of an ad hoc statement. Each test drives the same steps the runner takes for one run
/// (partition, fetch the misses, record the fetched shapes, share them with the run's other rows of the shape, settle
/// after the commit) over several runs of one workload, and counts the plans fetched and the distinct digests stored.
/// </summary>
public sealed class QueryStatsPlanShapeReuseTests
{
    private const int ServerId = 7;
    private static readonly DateTime Start = new(2026, 11, 18, 8, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Created = new(2026, 11, 18, 7, 0, 0, DateTimeKind.Utc);

    private int _nextHandle;

    /// <summary>One execution of a statement: a fresh plan_handle every time, as an ad hoc literal variant compiles.</summary>
    private QueryStatsCollector.Row AdHoc(int shape, string planHash = "", string database = "app") => new()
    {
        PlanHandle = "0x06" + (++_nextHandle).ToString("X8", CultureInfo.InvariantCulture),
        DatabaseName = database,
        CreationTime = Created,
        PlanGenerationNum = 1,
        StatementStartOffset = 0,
        StatementEndOffset = -1,
        QueryHash = "0xA" + shape.ToString("X15", CultureInfo.InvariantCulture),
        QueryPlanHash = planHash.Length > 0 ? planHash : "0xB" + shape.ToString("X15", CultureInfo.InvariantCulture),
    };

    /// <summary>A procedure statement: the same plan_handle on every run, so its identity stays cached.</summary>
    private static QueryStatsCollector.Row Procedure(int n) => new()
    {
        PlanHandle = "0x05" + n.ToString("X8", CultureInfo.InvariantCulture),
        DatabaseName = "app",
        CreationTime = Created,
        PlanGenerationNum = 1,
        StatementStartOffset = 0,
        StatementEndOffset = -1,
        QueryHash = "0xC" + n.ToString("X15", CultureInfo.InvariantCulture),
        QueryPlanHash = "0xD" + n.ToString("X15", CultureInfo.InvariantCulture),
    };

    private sealed class Store
    {
        public readonly PlanDigestCache<QueryStatsPlanKey> Identities = new();
        public readonly PlanDigestCache<QueryStatsPlanShapeKey> Shapes = new();
        public readonly HashSet<string> Digests = new(StringComparer.Ordinal);
    }

    /// <summary>
    /// One run. The target renders each fetched plan with the row's own plan_handle in it, standing in for the literals
    /// an ad hoc plan's XML carries, so two variants of one shape render two different plans (two digests). Returns how
    /// many plans the run fetched.
    /// </summary>
    private static int Run(Store store, List<QueryStatsCollector.Row> rows, DateTime now)
    {
        var misses = DarlingCollectorRunner.PartitionDeferredQueryStatsPlans(ServerId, rows, store.Identities, now, store.Shapes);

        var pending = new List<QueryStatsPlanKey>();
        var pendingShapes = new List<QueryStatsPlanShapeKey>();
        var fetchedByShape = new Dictionary<QueryStatsPlanShapeKey, (string PlanXml, long? Bytes)>();
        foreach (var (key, row) in misses)
        {
            row.QueryPlanXml = "<ShowPlanXML><StmtSimple StatementText=\"" + row.PlanHandle + "\" /></ShowPlanXML>";
            row.QueryPlanXmlBytes = row.QueryPlanXml.Length;
            var digest = "D" + row.PlanHandle;
            store.Identities.AddPending(key, digest, row.QueryPlanXmlBytes, now);
            pending.Add(key);
            DarlingCollectorRunner.RecordFetchedShape(ServerId, row, digest, store.Shapes, pendingShapes, fetchedByShape, now);
        }

        DarlingCollectorRunner.ShareFetchedPlansByShape(ServerId, rows, fetchedByShape);

        /* What the batch stores: one dimension row per distinct plan text a row carries, and nothing for a known digest. */
        foreach (var row in rows.Where(r => r.QueryPlanXml is not null))
        {
            store.Digests.Add("D" + row.QueryPlanXml);
        }

        store.Identities.Settle(pending, committed: true, now);
        store.Shapes.Settle(pendingShapes, committed: true, now);
        return misses.Count;
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
    public void TenRunsOfLiteralOnlyVariants_StoreOnePlanPerShape_AndThenFetchNothing()
    {
        var store = new Store();
        var fetched = new List<int>();

        for (var run = 0; run < 10; run++)
        {
            /* 5 ad hoc shapes run 60 times each with new literals, plus 20 procedures: 320 rows a run, all executed. */
            var rows = Workload(shapes: 5, variantsPerShape: 60, procedures: 20);
            fetched.Add(Run(store, rows, Start.AddMinutes(run)));

            Assert.All(rows, r => Assert.True(r.QueryPlanXml is not null || r.KnownPlanDigest is not null));
        }

        /* Run one fetches one plan per shape and per procedure; every later run's new variants link to those. */
        Assert.Equal(25, fetched[0]);
        Assert.All(fetched.Skip(1), n => Assert.Equal(0, n));
        Assert.Equal(25, store.Digests.Count);
    }

    [Fact]
    public void ANewPlanShape_ForAStatementAlreadyStored_IsStillFetchedAndStored()
    {
        var store = new Store();
        Run(store, Workload(shapes: 1, variantsPerShape: 10, procedures: 0), Start);

        var recompiled = AdHoc(0, planHash: "0xBEEF000000000001");
        var sameShape = AdHoc(0);
        var fetched = Run(store, new List<QueryStatsCollector.Row> { recompiled, sameShape }, Start.AddMinutes(1));

        Assert.Equal(1, fetched);
        Assert.NotNull(recompiled.QueryPlanXml);
        Assert.Null(sameShape.QueryPlanXml);
        Assert.NotNull(sameShape.KnownPlanDigest);
        Assert.Equal(2, store.Digests.Count);
    }

    [Fact]
    public void TheSameHashesInTwoDatabases_AreTwoShapes()
    {
        var store = new Store();
        var rows = new List<QueryStatsCollector.Row> { AdHoc(0, database: "tenant1"), AdHoc(0, database: "tenant2") };

        Assert.Equal(2, Run(store, rows, Start));
        Assert.Equal(2, store.Digests.Count);
    }

    [Fact]
    public void AShapeWhoseStoredPlanAgedOut_IsFetchedAgainOnItsNextSighting()
    {
        var store = new Store();
        var first = AdHoc(0);
        Run(store, new List<QueryStatsCollector.Row> { first }, Start);

        /* The store reported the digest absent (its dimension row aged out): the runner evicts it from both caches. */
        store.Identities.Evict(new[] { "D" + first.PlanHandle });
        store.Shapes.Evict(new[] { "D" + first.PlanHandle });

        Assert.Equal(1, Run(store, new List<QueryStatsCollector.Row> { AdHoc(0) }, Start.AddMinutes(1)));
    }

    [Fact]
    public void ABatchThatFailed_LeavesTheShapeUnknown_SoTheNextRunFetchesIt()
    {
        var store = new Store();
        var row = AdHoc(0);
        var misses = DarlingCollectorRunner.PartitionDeferredQueryStatsPlans(
            ServerId, new[] { row }, store.Identities, Start, store.Shapes);
        var pendingShapes = new List<QueryStatsPlanShapeKey>();
        row.QueryPlanXml = "<ShowPlanXML />";
        row.QueryPlanXmlBytes = 15;
        DarlingCollectorRunner.RecordFetchedShape(
            ServerId, row, "D1", store.Shapes, pendingShapes, new Dictionary<QueryStatsPlanShapeKey, (string, long?)>(), Start);
        store.Shapes.Settle(pendingShapes, committed: false, Start);

        Assert.Single(misses);
        Assert.Equal(1, Run(store, new List<QueryStatsCollector.Row> { AdHoc(0) }, Start.AddMinutes(1)));
    }

    [Fact]
    public void RowsWithNoUsableShape_AreFetchedOneEach_AsBefore()
    {
        var store = new Store();
        var rows = Enumerable.Range(0, 3).Select(_ => AdHoc(0, planHash: "0x0000000000000000")).ToList();

        Assert.Equal(3, Run(store, rows, Start));
    }
}
