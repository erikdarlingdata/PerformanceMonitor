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
    private static CollectorContext MakeContext(bool capture, bool defer = false, bool azure = false)
        => new()
        {
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
    public void MainQuery_Deferred_CarriesNoPlanRender_AndNoPlanColumns(bool azure)
    {
        var sql = Build(MakeContext(capture: true, defer: true, azure: azure));

        Assert.DoesNotContain("dm_exec_text_query_plan", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("query_plan_xml", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void MainQuery_Deferred_IsTheNoPlanForm_PlusOnlyTheIdentityFragments(bool azure)
    {
        /* The checkout's line endings are CRLF on every OS (.gitattributes), so compare as LF. */
        var deferred = Build(MakeContext(capture: true, defer: true, azure: azure)).ReplaceLineEndings("\n");
        var captureOff = Build(MakeContext(capture: false, azure: azure)).ReplaceLineEndings("\n");

        Assert.Contains("plan_statement_count", deferred, StringComparison.Ordinal);
        Assert.Contains("plan_last_statement_compile", deferred, StringComparison.Ordinal);
        Assert.Contains("plan_generation_sum", deferred, StringComparison.Ordinal);
        Assert.Contains("OUTER APPLY", deferred, StringComparison.Ordinal);
        Assert.Contains("sys.dm_exec_query_stats", deferred, StringComparison.Ordinal);

        /* Cut the select fragment (from its leading comma through its last column) and the apply fragment
           (from its newline through the alias) and what remains is the capture-off text. */
        var selectStart = deferred.IndexOf(",\n    plan_statement_count", StringComparison.Ordinal);
        Assert.True(selectStart >= 0);
        var stripped = Strip(deferred, selectStart, "plan_generation_sum = pfp.plan_generation_sum");
        var applyStart = stripped.IndexOf("OUTER APPLY\n(", StringComparison.Ordinal);
        Assert.True(applyStart > 0);
        /* The apply fragment starts with a newline that follows `) AS ranked`; remove that newline too. */
        applyStart = stripped.LastIndexOf('\n', applyStart - 1) is var nl && nl >= 0 && stripped[nl..applyStart].Trim().Length == 0
            ? nl
            : applyStart;
        stripped = Strip(stripped, applyStart, ") AS pfp");

        Assert.Equal(captureOff, stripped);
    }

    /// <summary>
    /// The standard query nests the identity fragments inside N'...' dynamic SQL, so a single quote in either
    /// one would end the literal early. What the deferred text adds over capture-off carries none.
    /// </summary>
    [Fact]
    public void IdentityFragments_CarryNoSingleQuote_ForTheStandardNestedVariant()
    {
        var deferred = Build(MakeContext(capture: true, defer: true)).ReplaceLineEndings("\n");
        var captureOff = Build(MakeContext(capture: false)).ReplaceLineEndings("\n");

        var selectStart = deferred.IndexOf(",\n    plan_statement_count", StringComparison.Ordinal);
        Assert.True(selectStart >= 0);
        var selectEnd = deferred.IndexOf("plan_generation_sum = pfp.plan_generation_sum", selectStart, StringComparison.Ordinal);
        Assert.True(selectEnd >= 0);
        var applyStart = deferred.IndexOf("OUTER APPLY\n(", StringComparison.Ordinal);
        Assert.True(applyStart >= 0);
        var applyEnd = deferred.IndexOf(") AS pfp", applyStart, StringComparison.Ordinal);
        Assert.True(applyEnd >= 0);

        var added = deferred[selectStart..selectEnd] + deferred[applyStart..applyEnd];
        Assert.Contains("plan_statement_count", added, StringComparison.Ordinal);
        Assert.Contains("COUNT_BIG", added, StringComparison.Ordinal);
        Assert.DoesNotContain("'", added, StringComparison.Ordinal);
        Assert.Equal(captureOff.Split('\'').Length, deferred.Split('\'').Length);
    }

    private static string Strip(string text, int start, string endMarker)
    {
        var end = text.IndexOf(endMarker, start, StringComparison.Ordinal);
        Assert.True(end >= 0);
        return text.Remove(start, end + endMarker.Length - start);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void MainQuery_InlineCapture_StillCarriesTheModuleGrainPlanFetch(bool azure)
    {
        var sql = Build(MakeContext(capture: true, azure: azure));

        Assert.Contains("OUTER APPLY sys.dm_exec_text_query_plan(CONVERT(varbinary(64), ranked.plan_handle, 1), 0, -1) AS tqp",
            sql, StringComparison.Ordinal);
        Assert.Contains("query_plan_xml_bytes", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("plan_statement_count", sql, StringComparison.Ordinal);
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
    public async Task ReadAsync_Deferred_ReadsIdentityOrdinals_NotPlanOrdinals()
    {
        var compile = new DateTime(2026, 7, 1, 8, 30, 0, DateTimeKind.Utc);

        var deferred = await ReadOneRowAsync(MakeContext(capture: true, defer: true), deferredShape: true, compile);
        Assert.Null(deferred.QueryPlanXml);
        Assert.Null(deferred.QueryPlanXmlBytes);
        Assert.Null(deferred.KnownPlanDigest);
        Assert.Equal(7L, deferred.PlanStatementCount);
        Assert.Equal(compile, deferred.PlanLastStatementCompile);
        Assert.Equal(19L, deferred.PlanGenerationSum);

        var inline = await ReadOneRowAsync(MakeContext(capture: true), deferredShape: false, compile);
        Assert.Equal("<ShowPlanXML/>", inline.QueryPlanXml);
        Assert.Equal(14L, inline.QueryPlanXmlBytes);
        Assert.Null(inline.PlanStatementCount);
        Assert.Null(inline.PlanGenerationSum);

        var off = await ReadOneRowAsync(MakeContext(capture: false), deferredShape: false, compile, extra: false);
        Assert.Null(off.QueryPlanXml);
        Assert.Null(off.PlanStatementCount);
    }

    private static async Task<ProcedureStatsCollector.Row> ReadOneRowAsync(
        CollectorContext context, bool deferredShape, DateTime compile, bool extra = true)
    {
        using var table = new DataTable();
        for (var i = 0; i < 27; i++)
        {
            table.Columns.Add("c" + i.ToString(CultureInfo.InvariantCulture), ColumnType(i));
        }

        if (extra && deferredShape)
        {
            table.Columns.Add("plan_statement_count", typeof(long));
            table.Columns.Add("plan_last_statement_compile", typeof(DateTime));
            table.Columns.Add("plan_generation_sum", typeof(long));
        }
        else if (extra)
        {
            table.Columns.Add("query_plan_xml", typeof(string));
            table.Columns.Add("query_plan_xml_bytes", typeof(long));
        }

        var values = new object[table.Columns.Count];
        for (var i = 0; i < 27; i++)
        {
            values[i] = ColumnType(i) == typeof(long) && (i is 6 or 7 or 8 or 9 or 10 or 11 or 22 or 23 or 24)
                ? 1L
                : DBNull.Value;
        }

        if (extra && deferredShape)
        {
            values[27] = 7L;
            values[28] = compile;
            values[29] = 19L;
        }
        else if (extra)
        {
            values[27] = "<ShowPlanXML/>";
            values[28] = 14L;
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
