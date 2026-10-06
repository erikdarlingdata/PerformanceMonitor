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
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348 (statement filter, lane R2): the collection census for <c>query_stats</c> and <c>procedure_stats</c>. The
/// canary is planted in the collector's INPUT (a reader over a <see cref="DataTable"/>, as
/// <see cref="QueryStatsPlanFetchTests"/> and <see cref="ProcedureStatsPlanReuseTests"/> do), the read and the write
/// run, and <see cref="StatementScrubRecordingWriter"/> remembers what was written: no written value holds a secret
/// needle, the statement column is exactly the marker, the plan parses with the marker in statement 1 and the kept
/// needles in statements 2 and 3, and a plain statement is the same instance the reader returned.
/// </summary>
public sealed partial class StatementCollectionCensusTests
{
    private static readonly DateTime s_now = new(2026, 7, 2, 12, 0, 0, DateTimeKind.Utc);

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

    private static CollectorContext Context(bool capture, bool defer = false, bool identity = false) => new()
    {
        ServerId = 42,
        ServerName = "test-server",
        CollectionTime = s_now,
        Deltas = new NoDeltas(),
        Target = new CollectorTargetInfo(),
        CapturePlanXml = capture,
        DeferPlanXmlFetch = defer,
        PlanIdentityColumns = identity,
    };

    private static Type QueryStatsColumnType(int ordinal) => ordinal switch
    {
        3 or 4 => typeof(DateTime),
        0 or 1 or 2 or 36 or 37 or 38 or 42 => typeof(string),
        40 or 41 or 43 => typeof(int),
        _ => typeof(long),
    };

    /// <summary>A query_stats result with the given statement text and (optionally) inline plan columns.</summary>
    private static async Task<List<QueryStatsCollector.Row>> ReadQueryStatsAsync(
        CollectorContext context, params (string? Text, string? Plan)[] rows)
    {
        using var table = new DataTable();
        for (var i = 0; i < 44; i++)
        {
            table.Columns.Add("c" + i.ToString(CultureInfo.InvariantCulture), QueryStatsColumnType(i));
        }

        var withPlan = context.CapturePlanXml && !context.DeferPlanXmlFetch;
        if (withPlan)
        {
            table.Columns.Add("query_plan_xml", typeof(string));
            table.Columns.Add("query_plan_xml_bytes", typeof(long));
        }

        var n = 0;
        foreach (var (text, plan) in rows)
        {
            var values = new object[table.Columns.Count];
            for (var i = 0; i < 44; i++)
            {
                values[i] = DBNull.Value;
            }

            values[1] = "0xQ" + (n++).ToString(CultureInfo.InvariantCulture);
            values[36] = "0x01";
            values[37] = "0x02";
            values[38] = (object?)text ?? DBNull.Value;
            if (withPlan)
            {
                values[44] = (object?)plan ?? DBNull.Value;
                values[45] = plan is null ? DBNull.Value : (long)plan.Length;
            }

            table.Rows.Add(values);
        }

        await using var reader = table.CreateDataReader();
        return await QueryStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
    }

    private static StatementScrubRecordingWriter Write(QueryStatsCollector.Row row, CollectorContext context)
    {
        var writer = new StatementScrubRecordingWriter();
        QueryStatsCollector.Instance.WritePayload(row, writer, context);
        return writer;
    }

    [Fact]
    public async Task QueryStats_QueryText_IsWithheld()
    {
        var context = Context(capture: false);
        var plain = new string(StatementScrubCanary.PlainStatement.ToCharArray());

        var rows = await ReadQueryStatsAsync(context, (StatementScrubCanary.CanaryStatement, null), (plain, null));

        var named = Write(rows[0], context);
        foreach (var needle in StatementScrubCanary.SecretNeedles)
        {
            Assert.False(named.AnyStringContains(needle), needle);
        }

        /* the written value of the query_text column is exactly the marker */
        var textColumn = QueryStatsCollector.Instance.PayloadColumns.ToList().FindIndex(c => c.Name == "query_text");
        Assert.True(textColumn >= 0);
        Assert.Equal(SensitiveStatements.PlaceholderText, named.Values[textColumn]);
        Assert.Equal(SensitiveStatements.PlaceholderText, rows[0].QueryText);

        /* the plain statement is kept (its same-instance pin is the next test) */
        Assert.Equal(plain, rows[1].QueryText);
        Assert.Contains(plain, Write(rows[1], context).Strings);
    }

    [Fact]
    public async Task QueryStats_QueryText_PlainStatementIsTheSameInstanceTheReaderReturned()
    {
        var context = Context(capture: false);
        var plain = new string(StatementScrubCanary.PlainStatement.ToCharArray());
        using var table = new DataTable();
        for (var i = 0; i < 44; i++)
        {
            table.Columns.Add("c" + i.ToString(CultureInfo.InvariantCulture), QueryStatsColumnType(i));
        }

        var values = new object[44];
        for (var i = 0; i < 44; i++)
        {
            values[i] = DBNull.Value;
        }

        values[38] = plain;
        table.Rows.Add(values);
        await using var reader = table.CreateDataReader();

        var rows = await QueryStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Same(plain, Assert.Single(rows).QueryText);
    }

    [Fact]
    public async Task QueryStats_QueryPlanXml_InlinePlanIsFiltered()
    {
        var context = Context(capture: true);
        var rows = await ReadQueryStatsAsync(context, (null, StatementScrubCanary.CanaryPlan()));

        var written = Write(rows[0], context);

        var plan = Assert.Single(written.Payloads).Content!;
        StatementFilterCensus.AssertPlanFilteredKeepsTheRest(StatementScrubCanary.CanaryPlan(), plan);
        foreach (var needle in StatementScrubCanary.SecretNeedles)
        {
            Assert.False(written.AnyStringContains(needle), needle);
        }
    }

    [Fact]
    public async Task QueryStats_QueryPlanXml_DeferredFetchPlanIsFiltered_AndTheDigestIsOfTheFilteredPlan()
    {
        var plan = StatementScrubCanary.CanaryPlan();
        var first = await FetchOneAsync(plan, new SensitiveStatements.Session());
        var second = await FetchOneAsync(plan, new SensitiveStatements.Session());

        StatementFilterCensus.AssertPlanFilteredKeepsTheRest(plan, first!);
        Assert.Equal(first, second);
        Assert.Equal(
            Convert.ToHexString(PayloadDimensions.Digest(first!)),
            Convert.ToHexString(PayloadDimensions.Digest(second!)));
    }

    [Fact]
    public async Task QueryStats_QueryPlanXml_ASpentBudgetGivesTheWholeMarker_AndTheNextSessionTheFilteredPlan()
    {
        var plan = StatementScrubCanary.CanaryPlan();
        var spent = SpentSession();
        Assert.True(spent.Spent);

        var whole = await FetchOneAsync(plan, spent);
        var next = await FetchOneAsync(plan, new SensitiveStatements.Session());

        Assert.Equal(SensitiveStatements.PlaceholderText, whole);
        Assert.True(QueryStatsCollector.IsWithheldWhole(whole));
        Assert.False(QueryStatsCollector.IsWithheldWhole(next));
        StatementFilterCensus.AssertPlanFilteredKeepsTheRest(plan, next!);
    }

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

    private static async Task<string?> FetchOneAsync(string plan, SensitiveStatements.Session session)
    {
        using var table = new DataTable();
        table.Columns.Add("ord", typeof(int));
        table.Columns.Add("query_plan_xml", typeof(string));
        table.Columns.Add("query_plan_xml_bytes", typeof(long));
        table.Rows.Add(0, plan, (long)plan.Length);
        await using var reader = table.CreateDataReader();

        var fetched = await QueryStatsCollector.ReadPlanFetchAsync(reader, session, CancellationToken.None);
        return fetched[0].PlanXml;
    }

    [Fact]
    public async Task ProcedureStats_QueryPlanXml_InlinePlanIsFiltered()
    {
        var context = Context(capture: true);
        using var table = new DataTable();
        for (var i = 0; i < 27; i++)
        {
            table.Columns.Add("c" + i.ToString(CultureInfo.InvariantCulture), i switch
            {
                4 or 5 => typeof(DateTime),
                0 or 1 or 2 or 3 or 25 or 26 => typeof(string),
                _ => typeof(long),
            });
        }

        table.Columns.Add("query_plan_xml", typeof(string));
        table.Columns.Add("query_plan_xml_bytes", typeof(long));
        var values = new object[table.Columns.Count];
        for (var i = 0; i < 27; i++)
        {
            values[i] = table.Columns[i].DataType == typeof(long) && i is 6 or 7 or 8 or 9 or 10 or 11 or 22 or 23 or 24 ? 1L : DBNull.Value;
        }

        values[27] = StatementScrubCanary.CanaryPlan();
        values[28] = 7L;
        table.Rows.Add(values);

        await using var reader = table.CreateDataReader();
        var rows = await ProcedureStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        var writer = new StatementScrubRecordingWriter();
        ProcedureStatsCollector.Instance.WritePayload(Assert.Single(rows), writer, context);

        var plan = Assert.Single(writer.Payloads).Content!;
        StatementFilterCensus.AssertPlanFilteredKeepsTheRest(StatementScrubCanary.CanaryPlan(), plan);
        foreach (var needle in StatementScrubCanary.SecretNeedles)
        {
            Assert.False(writer.AnyStringContains(needle), needle);
        }
    }

    /// <summary>r2 Q5: a plan the filter withheld whole (budget spent) is stored this cycle but never cached, so the next
    /// cycle fetches the plan again and stores its filtered form; a filtered plan IS cached under its own digest.</summary>
    [Fact]
    public async Task ProcedureStats_QueryPlanXml_AWholeMarkerPlanFromTheDeferredFetchIsNotCached_AndTheNextCycleCachesTheFilteredPlan()
    {
        var cache = new PlanDigestCache<ProcedureStatsPlanKey>();
        var plan = StatementScrubCanary.CanaryPlan();
        var row = default(ProcedureStatsCollector.Row) with
        {
            DatabaseName = "db1",
            SchemaName = "dbo",
            ObjectName = "p1",
            ObjectType = "P",
            CachedTime = s_now.AddHours(-4),
            SqlHandle = "0x0301",
            PlanHandle = "0x0501",
            PlanStatementCount = 3,
            PlanLastStatementCompile = s_now.AddHours(-3),
            PlanGenerationSum = 7,
        };

        var unjudged = new HashSet<int>();
        ProcedureStatsPlanReuse.PlanFetch FetchThrough(SensitiveStatements.Session session) => async (handles, token) =>
        {
            using var table = new DataTable();
            table.Columns.Add("ord", typeof(int));
            table.Columns.Add("query_plan_xml", typeof(string));
            table.Columns.Add("query_plan_xml_bytes", typeof(long));
            table.Rows.Add(0, plan, (long)plan.Length);
            await using var reader = table.CreateDataReader();
            return await QueryStatsCollector.ReadPlanFetchAsync(reader, session, token, unjudged);
        };

        var spent = SpentSession();
        var rows1 = new List<ProcedureStatsCollector.Row> { row };
        var first = await ProcedureStatsPlanReuse.ApplyOnAsync(1, cache, rows1, true, 1, s_now, FetchThrough(spent), CancellationToken.None, unjudged);
        cache.ConfirmPending(first.Pending, s_now);

        Assert.Equal(SensitiveStatements.PlaceholderText, rows1[0].QueryPlanXml);
        Assert.Empty(first.Pending);

        var rows2 = new List<ProcedureStatsCollector.Row> { row };
        unjudged.Clear();
        var second = await ProcedureStatsPlanReuse.ApplyOnAsync(1, cache, rows2, true, 2, s_now, FetchThrough(new SensitiveStatements.Session()), CancellationToken.None, unjudged);
        cache.ConfirmPending(second.Pending, s_now);

        Assert.Equal(0, second.Hit);
        StatementFilterCensus.AssertPlanFilteredKeepsTheRest(plan, rows2[0].QueryPlanXml!);
        Assert.Single(second.Pending);

        var rows3 = new List<ProcedureStatsCollector.Row> { row };
        var third = await ProcedureStatsPlanReuse.ApplyOnAsync(1, cache, rows3, true, 3, s_now, FetchThrough(new SensitiveStatements.Session()), CancellationToken.None, unjudged);
        Assert.Equal(1, third.Hit);
        Assert.Equal(ProcedureStatsPlanReuse.DigestOf(rows2[0].QueryPlanXml!), rows3[0].KnownPlanDigest);
    }

    /// <summary>A prepared plan cut inside its parameter list: it does not parse, and the auto-parameter probe could have
    /// mattered, so the filter withholds the whole plan without a budget being involved (the same plan every cycle).</summary>
    private const string CutPreparedPlan = "<ShowPlanXML><BatchSequence><Batch><Statements><StmtSimple StatementText=\"UPDATE t SET p = @1\">"
        + "<QueryPlan><ParameterList><ColumnReference Column=\"@1\" ParameterCompiledValue=\"N&apos;S3";

    /// <summary>#5367 review, A-L4: a plan the filter withheld whole for its OWN sake (it cannot be parsed) is not a
    /// budget skip. It fails the same way every cycle, so it is recorded like any fetched plan: the second cycle
    /// answers from the cache instead of fetching and rendering the plan on the target again.</summary>
    [Fact]
    public async Task ProcedureStats_QueryPlanXml_APlanWithheldWholeForItsOwnSakeIsCached_SoTheNextCycleDoesNotFetchItAgain()
    {
        var cache = new PlanDigestCache<ProcedureStatsPlanKey>();
        var plan = CutPreparedPlan;
        var row = default(ProcedureStatsCollector.Row) with
        {
            DatabaseName = "db1",
            SchemaName = "dbo",
            ObjectName = "p1",
            ObjectType = "P",
            CachedTime = s_now.AddHours(-4),
            SqlHandle = "0x0301",
            PlanHandle = "0x0501",
            PlanStatementCount = 3,
            PlanLastStatementCompile = s_now.AddHours(-3),
            PlanGenerationSum = 7,
        };

        var fetches = 0;
        var unjudged = new HashSet<int>();
        ProcedureStatsPlanReuse.PlanFetch FetchThrough(SensitiveStatements.Session session) => async (handles, token) =>
        {
            fetches++;
            using var table = new DataTable();
            table.Columns.Add("ord", typeof(int));
            table.Columns.Add("query_plan_xml", typeof(string));
            table.Columns.Add("query_plan_xml_bytes", typeof(long));
            table.Rows.Add(0, plan, (long)plan.Length);
            await using var reader = table.CreateDataReader();
            return await QueryStatsCollector.ReadPlanFetchAsync(reader, session, token, unjudged);
        };

        SensitiveStatements.Session Ordinary() => new();

        var rows1 = new List<ProcedureStatsCollector.Row> { row };
        var first = await ProcedureStatsPlanReuse.ApplyOnAsync(1, cache, rows1, true, 1, s_now, FetchThrough(Ordinary()), CancellationToken.None, unjudged);
        cache.ConfirmPending(first.Pending, s_now);

        Assert.Equal(SensitiveStatements.PlaceholderText, rows1[0].QueryPlanXml);
        Assert.Empty(unjudged);
        Assert.Single(first.Pending);

        var rows2 = new List<ProcedureStatsCollector.Row> { row };
        var second = await ProcedureStatsPlanReuse.ApplyOnAsync(1, cache, rows2, true, 2, s_now, FetchThrough(Ordinary()), CancellationToken.None, unjudged);

        Assert.Equal(1, second.Hit);
        Assert.Equal(1, fetches);
        Assert.Equal(ProcedureStatsPlanReuse.DigestOf(SensitiveStatements.PlaceholderText), rows2[0].KnownPlanDigest);
    }

    [Fact]
    public async Task ReadPlanFetchAsync_ReportsOnlyTheBudgetSkippedPlansAsUnjudged()
    {
        var plan = StatementScrubCanary.CanaryPlan();
        using var table = new DataTable();
        table.Columns.Add("ord", typeof(int));
        table.Columns.Add("query_plan_xml", typeof(string));
        table.Columns.Add("query_plan_xml_bytes", typeof(long));
        table.Rows.Add(0, plan, (long)plan.Length);
        using var cutTable = new DataTable();
        cutTable.Columns.Add("ord", typeof(int));
        cutTable.Columns.Add("query_plan_xml", typeof(string));
        cutTable.Columns.Add("query_plan_xml_bytes", typeof(long));
        cutTable.Rows.Add(0, CutPreparedPlan, (long)CutPreparedPlan.Length);

        var spentSet = new HashSet<int>();
        await using (var reader = table.CreateDataReader())
        {
            await QueryStatsCollector.ReadPlanFetchAsync(reader, SpentSession(), CancellationToken.None, spentSet);
        }

        var ownSakeSet = new HashSet<int>();
        await using (var reader = cutTable.CreateDataReader())
        {
            var fetched = await QueryStatsCollector.ReadPlanFetchAsync(reader, new SensitiveStatements.Session(), CancellationToken.None, ownSakeSet);
            Assert.Equal(SensitiveStatements.PlaceholderText, fetched[0].PlanXml);
        }

        Assert.Equal(new[] { 0 }, spentSet);
        Assert.Empty(ownSakeSet);
    }

    [Fact]
    public void QueryStats_TheDeferredFetch_CachesAWholeMarkerPlanUnlessTheBudgetSkippedIt()
    {
        var source = System.IO.File.ReadAllText(RepoFile.PathTo("Darling/PerformanceMonitor.Darling.Service/DarlingCollectorRunner.cs"));
        var bodies = StatementColumnCensusTests.BodiesOf(source, "DarlingCollectorRunner", "FetchDeferredQueryStatsPlansAsync");

        Assert.Single(bodies);
        Assert.Contains("unjudged.Contains(ord)", bodies[0], StringComparison.Ordinal);
        Assert.DoesNotContain("IsWithheldWhole", bodies[0], StringComparison.Ordinal);
    }
}
