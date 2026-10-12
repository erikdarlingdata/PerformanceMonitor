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
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5158: <c>query_stats</c> can leave the plan out of its main query (<see cref="CollectorContext.DeferPlanXmlFetch"/>)
/// and render only the plans a host asks for in a second target query. Nothing sets the switch yet, so
/// these pin the collector's two shapes and the writer seam, not any behavior change. The full-text
/// byte pins for the unchanged shapes are in <see cref="QueryStatsSqlGoldenTests"/>.
/// </summary>
public sealed class QueryStatsPlanFetchTests
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

    private static readonly QueryStatsCollector.PlanFetchKey[] s_keys =
    {
        new(new byte[] { 0x06, 0x00, 0x05, 0xAB }, 0, -1),
        new(new byte[] { 0x06, 0x00, 0x0F, 0x01, 0xEE }, 128, 340),
        new(new byte[] { 0xFF }, 0, 90),
    };

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void MainQuery_Deferred_CarriesNoPlanFetchAndNoPlanColumns(bool azure)
    {
        var sql = QueryStatsCollector.Instance.BuildQuery(MakeContext(capture: true, defer: true, azure: azure)).Text;

        Assert.DoesNotContain("dm_exec_text_query_plan", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("query_plan_xml", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void MainQuery_Deferred_IsTheNoPlanForm(bool azure)
    {
        var deferred = QueryStatsCollector.Instance.BuildQuery(MakeContext(capture: true, defer: true, azure: azure)).Text;
        var captureOff = QueryStatsCollector.Instance.BuildQuery(MakeContext(capture: false, azure: azure)).Text;

        Assert.Equal(captureOff, deferred);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void MainQuery_InlineCapture_StillCarriesThePlanFetch(bool azure)
    {
        var sql = QueryStatsCollector.Instance.BuildQuery(MakeContext(capture: true, azure: azure)).Text;

        Assert.Contains("dm_exec_text_query_plan", sql, StringComparison.Ordinal);
        Assert.Contains("query_plan_xml_bytes", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void DeferWithoutCapture_ChangesNothing()
    {
        Assert.Equal(
            QueryStatsCollector.Instance.BuildQuery(MakeContext(capture: false)).Text,
            QueryStatsCollector.Instance.BuildQuery(MakeContext(capture: false, defer: true)).Text);
    }

    [Fact]
    public void FetchQuery_HasTheValuesShape_TheCapCase_TheBytesColumn_AndRecompile()
    {
        var sql = QueryStatsCollector.BuildPlanFetchQuery(MakeContext(capture: true, defer: true), s_keys).Text;

        Assert.Contains("(0, 0x060005AB, 0, -1)", sql, StringComparison.Ordinal);
        Assert.Contains("(1, 0x06000F01EE, 128, 340)", sql, StringComparison.Ordinal);
        Assert.Contains("(2, 0xFF, 0, 90)", sql, StringComparison.Ordinal);
        Assert.True(sql.IndexOf("(0, 0x", StringComparison.Ordinal) < sql.IndexOf("(1, 0x", StringComparison.Ordinal));
        Assert.True(sql.IndexOf("(1, 0x", StringComparison.Ordinal) < sql.IndexOf("(2, 0x", StringComparison.Ordinal));
        Assert.Contains("AS k (ord, plan_handle, s, e)", sql, StringComparison.Ordinal);
        Assert.Contains("OUTER APPLY", sql, StringComparison.Ordinal);
        Assert.Contains("k.plan_handle,", sql, StringComparison.Ordinal);
        Assert.Contains("sys.dm_exec_text_query_plan", sql, StringComparison.Ordinal);
        Assert.Contains("query_plan_xml_bytes = DATALENGTH(tqp.query_plan)", sql, StringComparison.Ordinal);
        Assert.EndsWith("OPTION(RECOMPILE);", sql, StringComparison.Ordinal);
        Assert.Empty(QueryStatsCollector.BuildPlanFetchQuery(MakeContext(capture: true), s_keys).Parameters);
    }

    [Fact]
    public void FetchQuery_UsesTheSameCapCase_AsTheInlineFragment()
    {
        var cap = "CASE WHEN DATALENGTH(tqp.query_plan) > "
            + QueryPlanXmlCaptureLimits.MaxCapturedPlanXmlBytes.ToString(System.Globalization.CultureInfo.InvariantCulture)
            + " THEN NULL ELSE tqp.query_plan END";

        var inline = QueryStatsCollector.Instance.BuildQuery(MakeContext(capture: true)).Text;
        var fetch = QueryStatsCollector.BuildPlanFetchQuery(MakeContext(capture: true), s_keys).Text;

        Assert.Contains(cap, inline, StringComparison.Ordinal);
        Assert.Contains(cap, fetch, StringComparison.Ordinal);
    }

    [Fact]
    public void FetchQuery_ThrowsOnAnEmptyKeyList()
        => Assert.Throws<InvalidOperationException>(() =>
            QueryStatsCollector.BuildPlanFetchQuery(MakeContext(capture: true), Array.Empty<QueryStatsCollector.PlanFetchKey>()));

    [Fact]
    public void FetchQuery_ThrowsWhenCaptureIsOff()
        => Assert.Throws<InvalidOperationException>(() =>
            QueryStatsCollector.BuildPlanFetchQuery(MakeContext(capture: false), s_keys));

    [Theory]
    [InlineData(0)]
    [InlineData(65)]
    public void FetchQuery_ThrowsOnAHandleThatIsNotAPlanHandle(int length)
        => Assert.Throws<InvalidOperationException>(() =>
            QueryStatsCollector.BuildPlanFetchQuery(
                MakeContext(capture: true),
                new[] { new QueryStatsCollector.PlanFetchKey(new byte[length], 0, -1) }));

    [Fact]
    public async Task ReadPlanFetchAsync_MapsOrdToPlanAndBytes_IncludingAgedOutAndOverCap()
    {
        using var table = new DataTable();
        table.Columns.Add("ord", typeof(int));
        table.Columns.Add("query_plan_xml", typeof(string));
        table.Columns.Add("query_plan_xml_bytes", typeof(long));
        table.Rows.Add(0, "<ShowPlanXML/>", 14L);
        table.Rows.Add(1, DBNull.Value, 900_000L);
        table.Rows.Add(2, DBNull.Value, DBNull.Value);

        await using var reader = table.CreateDataReader();
        var result = await QueryStatsCollector.ReadPlanFetchAsync(reader, new PerformanceMonitor.Common.SensitiveStatements.Session(), CancellationToken.None);

        Assert.Equal(("<ShowPlanXML/>", (long?)14), result[0]);
        Assert.Equal(((string?)null, (long?)900_000), result[1]);
        Assert.Equal(((string?)null, (long?)null), result[2]);
    }

    /// <summary>The read goes through the collector's own ReadAsync, in both modes, over the shipped column layout.</summary>
    [Fact]
    public async Task ReadAsync_Deferred_DoesNotReadTheTrailingPlanOrdinals()
    {
        var deferred = await ReadOneRowAsync(MakeContext(capture: true, defer: true), withPlanColumns: false);
        Assert.Null(deferred.QueryPlanXml);
        Assert.Null(deferred.QueryPlanXmlBytes);
        Assert.Null(deferred.KnownPlanDigest);

        var inline = await ReadOneRowAsync(MakeContext(capture: true), withPlanColumns: true);
        Assert.Equal("<ShowPlanXML/>", inline.QueryPlanXml);
        Assert.Equal(14L, inline.QueryPlanXmlBytes);
    }

    private static async Task<QueryStatsCollector.Row> ReadOneRowAsync(CollectorContext context, bool withPlanColumns)
    {
        using var table = new DataTable();
        for (var i = 0; i < 44; i++)
        {
            /* The columns the read touches are typed by position; the rest only need to exist. */
            table.Columns.Add("c" + i.ToString(System.Globalization.CultureInfo.InvariantCulture), ColumnType(i));
        }

        if (withPlanColumns)
        {
            table.Columns.Add("query_plan_xml", typeof(string));
            table.Columns.Add("query_plan_xml_bytes", typeof(long));
        }

        var values = new object[table.Columns.Count];
        for (var i = 0; i < 44; i++)
        {
            values[i] = DBNull.Value;
        }

        if (withPlanColumns)
        {
            values[44] = "<ShowPlanXML/>";
            values[45] = 14L;
        }

        table.Rows.Add(values);

        await using var reader = table.CreateDataReader();
        var rows = await QueryStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        return Assert.Single(rows);
    }

    private static Type ColumnType(int ordinal) => ordinal switch
    {
        3 or 4 => typeof(DateTime),
        0 or 1 or 2 or 36 or 37 or 38 or 42 => typeof(string),
        40 or 41 or 43 => typeof(int),
        _ => typeof(long),
    };

    [Fact]
    public void PayloadOrDigest_Default_CallsValueExactlyOnceWithTheContent_AndIgnoresTheDigest()
    {
        var writer = new CountingWriter();

        var returned = ((ICollectorRowWriter)writer).PayloadOrDigest("<plan/>", "ABCDEF");

        Assert.Same(writer, returned);
        Assert.Equal(new[] { "<plan/>" }, writer.StringValues);
    }

    [Fact]
    public void PayloadOrDigest_Default_PassesANullContentThrough()
    {
        var writer = new CountingWriter();

        ((ICollectorRowWriter)writer).PayloadOrDigest(null, "ABCDEF");

        Assert.Equal(new string?[] { null }, writer.StringValues);
    }

    /// <summary>Wiring: WritePayload goes through PayloadOrDigest, so a digest-aware writer sees the row's digest.</summary>
    [Fact]
    public void WritePayload_RoutesThePlanColumnThroughPayloadOrDigest_WithTheRowsDigest()
    {
        var writer = new DigestAwareWriter();
        var row = new QueryStatsCollector.Row
        {
            QueryHash = "0x01",
            QueryPlanXml = "<plan/>",
            KnownPlanDigest = "ABCDEF",
        };

        QueryStatsCollector.Instance.WritePayload(row, writer, MakeContext(capture: true));

        var call = Assert.Single(writer.PayloadOrDigestCalls);
        Assert.Equal(("<plan/>", "ABCDEF"), call);
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

    private class CountingWriter : ICollectorRowWriter
    {
        public List<string?> StringValues { get; } = new();
        public ICollectorRowWriter Value(string? value) { StringValues.Add(value); return this; }
        public ICollectorRowWriter Value(long value) => this;
        public ICollectorRowWriter Value(long? value) => this;
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
    }

    private sealed class DigestAwareWriter : CountingWriter, ICollectorRowWriter
    {
        public List<(string? Content, string? Digest)> PayloadOrDigestCalls { get; } = new();

        ICollectorRowWriter ICollectorRowWriter.PayloadOrDigest(string? content, string? knownDigest)
        {
            PayloadOrDigestCalls.Add((content, knownDigest));
            return this;
        }
    }
}
