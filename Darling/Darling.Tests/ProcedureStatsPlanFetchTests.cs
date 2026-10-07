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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5158: <c>procedure_stats</c> can leave the plan out of its main query
/// (<see cref="CollectorContext.DeferPlanXmlFetch"/>), read a three-column identity fingerprint in its place,
/// and render plans in a second target query. Nothing sets the switch yet, so these pin the collector's
/// shapes and the writer seam, not a behavior change. The byte pins for the unchanged shapes are in
/// <see cref="ProcedureStatsSqlGoldenTests"/>.
/// </summary>
public sealed class ProcedureStatsPlanFetchTests
{
    private static CollectorContext MakeContext(bool capture, bool defer = false, bool azure = false, bool identity = false)
        => new()
        {
            PlanIdentityColumns = identity,
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = new DateTime(2026, 7, 2, 12, 0, 0, DateTimeKind.Utc),
            Deltas = new NoDeltas(),
            Target = new CollectorTargetInfo { IsAzureSqlDb = azure },
            CapturePlanXml = capture,
            DeferPlanXmlFetch = defer,
        };

    private static readonly byte[][] s_handles =
    {
        new byte[] { 0x05, 0x00, 0x05, 0xAB },
        new byte[] { 0x05, 0x00, 0x0F, 0x01, 0xEE },
    };

    private static string Build(CollectorContext c) => ProcedureStatsCollector.Instance.BuildQuery(c).Text;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void MainQuery_IsNumbersOnly_InEveryPlanMode(bool azure)
    {
        /* #5449: no plan columns, no identity columns, no apply and no derived table, whatever the mode. */
        foreach (var context in new[]
        {
            MakeContext(capture: false, azure: azure),
            MakeContext(capture: true, azure: azure),
            MakeContext(capture: true, azure: azure, identity: true),
            MakeContext(capture: true, defer: true, azure: azure, identity: true),
            MakeContext(capture: false, azure: azure, identity: true),
        })
        {
            var sql = Build(context);

            Assert.DoesNotContain("dm_exec_text_query_plan", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("query_plan_xml", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("plan_statement_count", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("ranked", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("TOP (150)", sql, StringComparison.Ordinal);
            Assert.Contains("TOP (" + ProcedureStatsCollector.MaxCandidateRows + ")", sql, StringComparison.Ordinal);
            Assert.Contains("last_execution_time DESC", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("PLAN_SELECT", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("PLAN_APPLY", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheCandidateCap_IsOneConstant_BesideThePlanLimit()
    {
        Assert.Equal(5000, ProcedureStatsCollector.MaxCandidateRows);
        Assert.Equal(150, ProcedureStatsCollector.MaxPlansPerRun);
        Assert.Equal(ProcedureStatsCollector.MaxPlansPerRun, PerformanceMonitor.Darling.Service.ProcedureStatsPlanReuse.MaxMissesPerRun);

        /* The standard text names the cap once, in the dynamic SQL; the Azure text names it once. */
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(Build(MakeContext(capture: false)), @"TOP \(\d+\)"));
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(Build(MakeContext(capture: false, azure: true)), @"TOP \(\d+\)"));
    }

    private static string PlanPhase(CollectorContext c) => ProcedureStatsCollector.BuildPlanPhaseQuery(c, s_handles).Text.ReplaceLineEndings("\n");

    [Fact]
    public void PlanPhase_Off_AppliesThePlanFragmentOverTheValuesList()
    {
        var sql = PlanPhase(MakeContext(capture: true));

        Assert.Contains("OUTER APPLY sys.dm_exec_text_query_plan(CONVERT(varbinary(64), ranked.plan_handle, 1), 0, -1) AS tqp", sql, StringComparison.Ordinal);
        Assert.Contains("query_plan_xml_bytes", sql, StringComparison.Ordinal);
        Assert.Contains(") AS ranked (ord, plan_handle)", sql, StringComparison.Ordinal);
        Assert.Contains("(0, '0x050005AB')", sql, StringComparison.Ordinal);
        Assert.Contains("(1, '0x05000F01EE')", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("plan_statement_count", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void PlanPhase_Shadow_AppliesBothFragments_PlanFirst()
    {
        var sql = PlanPhase(MakeContext(capture: true, identity: true));

        Assert.Contains("dm_exec_text_query_plan", sql, StringComparison.Ordinal);
        Assert.Contains("plan_statement_count = pfp.plan_statement_count", sql, StringComparison.Ordinal);
        Assert.True(sql.IndexOf("query_plan_xml_bytes", StringComparison.Ordinal) < sql.IndexOf("plan_statement_count", StringComparison.Ordinal));
    }

    [Fact]
    public void PlanPhase_On_AppliesTheIdentityFragmentAlone()
    {
        foreach (var context in new[] { MakeContext(capture: true, defer: true, identity: true), MakeContext(capture: false, identity: true) })
        {
            var sql = PlanPhase(context);

            Assert.DoesNotContain("dm_exec_text_query_plan", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("query_plan_xml", sql, StringComparison.Ordinal);
            Assert.Contains("sys.dm_exec_query_stats", sql, StringComparison.Ordinal);
            Assert.Contains("WHERE qs.plan_handle = CONVERT(varbinary(64), ranked.plan_handle, 1)", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void PlanPhase_Throws_OnEmpty_TooMany_ABadHandle_OrARunWithNoPlanPhase()
    {
        Assert.Throws<InvalidOperationException>(() =>
            ProcedureStatsCollector.BuildPlanPhaseQuery(MakeContext(capture: true), Array.Empty<byte[]>()));
        Assert.Throws<InvalidOperationException>(() =>
            ProcedureStatsCollector.BuildPlanPhaseQuery(MakeContext(capture: false), s_handles));
        Assert.Throws<InvalidOperationException>(() =>
            ProcedureStatsCollector.BuildPlanPhaseQuery(MakeContext(capture: true), new[] { new byte[65] }));
        Assert.Throws<InvalidOperationException>(() =>
            ProcedureStatsCollector.BuildPlanPhaseQuery(MakeContext(capture: true), new[] { Array.Empty<byte>() }));
        Assert.Throws<InvalidOperationException>(() =>
            ProcedureStatsCollector.BuildPlanPhaseQuery(
                MakeContext(capture: true), System.Linq.Enumerable.Repeat(new byte[] { 1 }, ProcedureStatsCollector.MaxPlansPerRun + 1).ToArray()));

        /* Exactly the limit is fine. */
        _ = ProcedureStatsCollector.BuildPlanPhaseQuery(
            MakeContext(capture: true), System.Linq.Enumerable.Repeat(new byte[] { 1 }, ProcedureStatsCollector.MaxPlansPerRun).ToArray());
    }

    [Fact]
    public void PlanPhaseApplies_OnlyWhenPlansOrIdentitiesAreAsked()
    {
        Assert.False(ProcedureStatsCollector.PlanPhaseApplies(MakeContext(capture: false)));
        Assert.True(ProcedureStatsCollector.PlanPhaseApplies(MakeContext(capture: true)));
        Assert.True(ProcedureStatsCollector.PlanPhaseApplies(MakeContext(capture: true, identity: true)));
        Assert.True(ProcedureStatsCollector.PlanPhaseApplies(MakeContext(capture: false, identity: true)));
        Assert.True(ProcedureStatsCollector.PlanPhaseApplies(MakeContext(capture: true, defer: true, identity: true)));
    }

    [Fact]
    public void SelectPlanPhaseRows_TakesTheFirst150ParsableHandles_InRowOrder()
    {
        var rows = new List<ProcedureStatsCollector.Row>();
        for (var i = 0; i < 200; i++)
        {
            /* Every tenth row has a handle that does not parse; it is never sent. */
            rows.Add(default(ProcedureStatsCollector.Row) with { PlanHandle = i % 10 == 3 ? "garbage" : "0x" + i.ToString("X4", CultureInfo.InvariantCulture) });
        }

        var indexes = ProcedureStatsCollector.SelectPlanPhaseRows(rows, out var handles);

        Assert.Equal(150, indexes.Count);
        Assert.Equal(150, handles.Count);
        Assert.DoesNotContain(3, indexes);
        Assert.Equal(new[] { 0, 1, 2, 4 }, indexes.GetRange(0, 4));
        Assert.Equal(new byte[] { 0x00, 0x04 }, handles[3]);
        Assert.True(indexes.SequenceEqual(indexes.OrderBy(x => x)));
    }

    [Fact]
    public void ApplyPlanPhase_MergesByPosition_AndMarksEveryOtherRowSkipped()
    {
        var rows = new List<ProcedureStatsCollector.Row>
        {
            default(ProcedureStatsCollector.Row) with { PlanHandle = "0x01" },
            default(ProcedureStatsCollector.Row) with { PlanHandle = "0x02" },
            default(ProcedureStatsCollector.Row) with { PlanHandle = "garbage" },
        };
        var compile = new DateTime(2026, 7, 1, 8, 30, 0, DateTimeKind.Utc);
        var results = new Dictionary<int, ProcedureStatsCollector.PlanPhaseResult>
        {
            [0] = new("<p/>", 4L, 7L, compile, 19L),
        };

        ProcedureStatsCollector.ApplyPlanPhase(rows, new[] { 0, 1 }, results);

        Assert.Equal("<p/>", rows[0].QueryPlanXml);
        Assert.Equal(4L, rows[0].QueryPlanXmlBytes);
        Assert.Equal(7L, rows[0].PlanStatementCount);
        Assert.Equal(compile, rows[0].PlanLastStatementCompile);
        Assert.Equal(19L, rows[0].PlanGenerationSum);
        Assert.False(rows[0].PlanPhaseSkipped);
        Assert.True(rows[1].PlanPhaseSkipped); /* sent, but no result came back */
        Assert.True(rows[2].PlanPhaseSkipped); /* never sent */

        /* A failed phase (null results) marks every row skipped and gives none a plan. */
        ProcedureStatsCollector.ApplyPlanPhase(rows, new[] { 0, 1 }, null);
        Assert.All(rows, r => Assert.True(r.PlanPhaseSkipped));
    }

    [Fact]
    public async Task ReadPlanPhaseAsync_ReadsTheColumnsOfTheRunsMode()
    {
        var compile = new DateTime(2026, 7, 1, 8, 30, 0, DateTimeKind.Utc);

        var on = await ReadPlanPhaseOneRowAsync(MakeContext(capture: true, defer: true, identity: true), "on", compile);
        Assert.Null(on.PlanXml);
        Assert.Null(on.PlanBytes);
        Assert.Equal(7L, on.StatementCount);
        Assert.Equal(compile, on.LastStatementCompile);
        Assert.Equal(19L, on.GenerationSum);

        var off = await ReadPlanPhaseOneRowAsync(MakeContext(capture: true), "off", compile);
        Assert.Equal("<ShowPlanXML/>", off.PlanXml);
        Assert.Equal(14L, off.PlanBytes);
        Assert.Null(off.StatementCount);
        Assert.Null(off.GenerationSum);

        var shadow = await ReadPlanPhaseOneRowAsync(MakeContext(capture: true, identity: true), "shadow", compile);
        Assert.Equal("<ShowPlanXML/>", shadow.PlanXml);
        Assert.Equal(14L, shadow.PlanBytes);
        Assert.Equal(7L, shadow.StatementCount);
        Assert.Equal(compile, shadow.LastStatementCompile);
        Assert.Equal(19L, shadow.GenerationSum);
    }

    private static async Task<ProcedureStatsCollector.PlanPhaseResult> ReadPlanPhaseOneRowAsync(CollectorContext context, string mode, DateTime compile)
    {
        using var table = new DataTable();
        table.Columns.Add("ord", typeof(int));
        if (mode != "on")
        {
            table.Columns.Add("query_plan_xml", typeof(string));
            table.Columns.Add("query_plan_xml_bytes", typeof(long));
        }

        if (mode != "off")
        {
            table.Columns.Add("plan_statement_count", typeof(long));
            table.Columns.Add("plan_last_statement_compile", typeof(DateTime));
            table.Columns.Add("plan_generation_sum", typeof(long));
        }

        var values = new List<object> { 0 };
        if (mode != "on")
        {
            values.Add("<ShowPlanXML/>");
            values.Add(14L);
        }

        if (mode != "off")
        {
            values.Add(7L);
            values.Add(compile);
            values.Add(19L);
        }

        table.Rows.Add(values.ToArray());

        await using var reader = table.CreateDataReader();
        var results = await ProcedureStatsCollector.ReadPlanPhaseAsync(reader, context, CancellationToken.None);
        return Assert.Single(results).Value;
    }

    [Fact]
    public void DeferWithoutCapture_ChangesNothing()
    {
        Assert.Equal(Build(MakeContext(capture: false)), Build(MakeContext(capture: false, defer: true)));
        Assert.Equal(Build(MakeContext(capture: false, azure: true)), Build(MakeContext(capture: false, defer: true, azure: true)));
    }

    [Fact]
    public void FetchQuery_PinsModuleOffsets_ZeroMinusOne()
    {
        Assert.Equal(0, ProcedureStatsCollector.ModuleStatementStartOffset);
        Assert.Equal(-1, ProcedureStatsCollector.ModuleStatementEndOffset);

        var sql = ProcedureStatsCollector.BuildPlanFetchQuery(MakeContext(capture: true, defer: true), s_handles).Text;

        Assert.Contains("(0, 0x050005AB, 0, -1)", sql, StringComparison.Ordinal);
        Assert.Contains("(1, 0x05000F01EE, 0, -1)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void FetchQuery_IsQueryStatsFetchQuery_ForTheSameKeys()
    {
        var ctx = MakeContext(capture: true, defer: true);
        var keys = new[]
        {
            new QueryStatsCollector.PlanFetchKey(s_handles[0], 0, -1),
            new QueryStatsCollector.PlanFetchKey(s_handles[1], 0, -1),
        };

        Assert.Equal(
            QueryStatsCollector.BuildPlanFetchQuery(ctx, keys).Text,
            ProcedureStatsCollector.BuildPlanFetchQuery(ctx, s_handles).Text);
    }

    [Fact]
    public void FetchQuery_Throws_OnEmpty_OrCaptureOff_OrABadHandle()
    {
        Assert.Throws<InvalidOperationException>(() =>
            ProcedureStatsCollector.BuildPlanFetchQuery(MakeContext(capture: true), Array.Empty<byte[]>()));
        Assert.Throws<InvalidOperationException>(() =>
            ProcedureStatsCollector.BuildPlanFetchQuery(MakeContext(capture: false), s_handles));
        Assert.Throws<InvalidOperationException>(() =>
            ProcedureStatsCollector.BuildPlanFetchQuery(MakeContext(capture: true), new[] { new byte[65] }));
        Assert.Throws<InvalidOperationException>(() =>
            ProcedureStatsCollector.BuildPlanFetchQuery(MakeContext(capture: true), new[] { Array.Empty<byte>() }));
    }

    [Theory]
    [InlineData("0x05", true, 1)]
    [InlineData("0x050005AB", true, 4)]
    [InlineData("0X0500", true, 2)]
    public void TryParsePlanHandle_AcceptsShippedHandleText(string hex, bool expected, int length)
    {
        Assert.Equal(expected, ProcedureStatsCollector.TryParsePlanHandle(hex, out var bytes));
        Assert.Equal(length, bytes.Length);
    }

    [Fact]
    public void TryParsePlanHandle_AcceptsExactly64Bytes_AndRejectsMore()
    {
        Assert.True(ProcedureStatsCollector.TryParsePlanHandle("0x" + new string('A', 128), out var ok));
        Assert.Equal(64, ok.Length);
        Assert.False(ProcedureStatsCollector.TryParsePlanHandle("0x" + new string('A', 130), out _));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("0x")]
    [InlineData("0x0")]
    [InlineData("0x050")]
    [InlineData("050005AB")]
    [InlineData("0x05ZZ")]
    public void TryParsePlanHandle_RejectsEverythingElse(string? hex)
    {
        Assert.False(ProcedureStatsCollector.TryParsePlanHandle(hex, out var bytes));
        Assert.Empty(bytes);
    }

    [Fact]
    public async Task ReadAsync_ReadsOrdinals0To26Only_NoPlanAndNoIdentity()
    {
        var row27 = await ReadOneRowAsync(MakeContext(capture: true, identity: true));
        Assert.Null(row27.QueryPlanXml);
        Assert.Null(row27.QueryPlanXmlBytes);
        Assert.Null(row27.PlanStatementCount);
        Assert.Null(row27.PlanGenerationSum);
        Assert.False(row27.PlanPhaseSkipped);
    }

    private static async Task<ProcedureStatsCollector.Row> ReadOneRowAsync(CollectorContext context)
    {
        using var table = new DataTable();
        for (var i = 0; i < 27; i++)
        {
            table.Columns.Add("c" + i.ToString(CultureInfo.InvariantCulture), ColumnType(i));
        }

        var values = new object[table.Columns.Count];
        for (var i = 0; i < 27; i++)
        {
            values[i] = ColumnType(i) == typeof(long) && (i is 6 or 7 or 8 or 9 or 10 or 11 or 22 or 23 or 24)
                ? 1L
                : DBNull.Value;
        }

        table.Rows.Add(values);

        await using var reader = table.CreateDataReader();
        var rows = await ProcedureStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        return Assert.Single(rows);
    }

    private static Type ColumnType(int ordinal) => ordinal switch
    {
        4 or 5 => typeof(DateTime),
        0 or 1 or 2 or 3 or 25 or 26 => typeof(string),
        _ => typeof(long),
    };

    private static ProcedureStatsCollector.Row SampleRow() => default(ProcedureStatsCollector.Row) with
    {
        DatabaseName = "db",
        SchemaName = "dbo",
        ObjectName = "p",
        ObjectType = "P",
        PlanHandle = "0x0500",
        QueryPlanXml = "<plan/>",
    };

    /// <summary>Wiring: WritePayload goes through PayloadOrDigest, so a digest-aware writer sees the row's digest.</summary>
    [Fact]
    public void WritePayload_RoutesThePlanColumnThroughPayloadOrDigest_WithTheRowsDigest()
    {
        var writer = new RecordingWriter();
        var row = SampleRow() with { KnownPlanDigest = "ABCDEF" };

        ProcedureStatsCollector.Instance.WritePayload(row, writer, MakeContext(capture: true));

        Assert.Equal(("<plan/>", "ABCDEF"), Assert.Single(writer.PayloadOrDigestCalls));
    }

    /// <summary>
    /// A deferred-fetch host sets QueryPlanXmlBytes from its cache for a known identity. A row with a known
    /// digest, no content and an over-cap size writes the digest through PayloadOrDigest, writes the bytes
    /// column, and still yields an oversized-plan observation, so the backlog's last_seen keeps refreshing.
    /// </summary>
    [Fact]
    public void KnownDigestWithOverCapBytes_WritesDigestAndBytes_AndYieldsAnOversizedObservation()
    {
        var writer = new RecordingWriter();
        var overCap = QueryPlanXmlCaptureLimits.MaxCapturedPlanXmlBytes + 1L;
        var row = SampleRow() with
        {
            QueryPlanXml = null,
            SqlHandle = "0x0300",
            KnownPlanDigest = "ABCDEF",
            QueryPlanXmlBytes = overCap,
        };

        ProcedureStatsCollector.Instance.WritePayload(row, writer, MakeContext(capture: true));
        var observation = ProcedureStatsCollector.Instance.DescribeOversizedPlan(row);

        Assert.Equal(((string?)null, "ABCDEF"), Assert.Single(writer.PayloadOrDigestCalls));
        Assert.Contains(overCap, writer.NullableLongs);
        Assert.NotNull(observation);
        Assert.Equal(overCap, observation!.Value.ObservedBytes);
    }

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

    private sealed class RecordingWriter : ICollectorRowWriter
    {
        public List<(string? Content, string? Digest)> PayloadOrDigestCalls { get; } = new();
        public List<long?> NullableLongs { get; } = new();
        public ICollectorRowWriter Value(string? value) => this;
        public ICollectorRowWriter Value(long value) => this;
        public ICollectorRowWriter Value(long? value) { NullableLongs.Add(value); return this; }
        public ICollectorRowWriter Value(int value) => this;
        public ICollectorRowWriter Value(int? value) => this;
        public ICollectorRowWriter Value(short value) => this;
        public ICollectorRowWriter Value(short? value) => this;
        public ICollectorRowWriter Value(double value) => this;
        public ICollectorRowWriter Value(double? value) => this;
        public ICollectorRowWriter Value(decimal value) => this;
        public ICollectorRowWriter Value(decimal? value) => this;
        public ICollectorRowWriter Value(bool value) => this;
        public ICollectorRowWriter Value(bool? value) => this;
        public ICollectorRowWriter Value(DateTime value) => this;
        public ICollectorRowWriter Value(DateTime? value) => this;
        public ICollectorRowWriter NullValue() => this;

        ICollectorRowWriter ICollectorRowWriter.PayloadOrDigest(string? content, string? knownDigest)
        {
            PayloadOrDigestCalls.Add((content, knownDigest));
            return this;
        }
    }
}
