/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins for the slow-read record's write rule and shaping (#5097): which reads are recorded, how arguments and
/// statements are normalised and bounded, the bounded queue, and the read tool's payload trim. The threshold is a
/// process-wide seam, so the class shares the live collection and runs serially with the acceptance test that lowers it.
/// </summary>
[Collection("live-postgres")]
public sealed class SlowReadLogTests
{
    private static ReadScope NewScope()
    {
        using var opened = ReadScope.Open(null);
        return opened.Scope;
    }

    [Fact]
    public void AReadIsRecorded_OverTheThreshold_OrOnATimeoutErrorOrLimit_AndNotOtherwise()
    {
        Assert.Equal(5_000, SlowReadLog.ThresholdMs);

        Assert.True(SlowReadLog.ShouldRecord(ReadOutcome.Ok, 5_000));
        Assert.False(SlowReadLog.ShouldRecord(ReadOutcome.Ok, 4_999));
        Assert.True(SlowReadLog.ShouldRecord(ReadOutcome.Timeout, 10));
        Assert.True(SlowReadLog.ShouldRecord(ReadOutcome.Error, 10));
        Assert.True(SlowReadLog.ShouldRecord(ReadOutcome.Limit, 10));
        Assert.False(SlowReadLog.ShouldRecord(ReadOutcome.Cancelled, 10));
        Assert.False(SlowReadLog.ShouldRecord(ReadOutcome.FallbackRaw, 10));
        Assert.False(SlowReadLog.ShouldRecord(ReadOutcome.GateFailed, 10));
        Assert.True(SlowReadLog.ShouldRecord(ReadOutcome.Cancelled, 6_000));
        Assert.True(SlowReadLog.ShouldRecord(ReadOutcome.FallbackRaw, 6_000));
    }

    [Fact]
    public void Offer_QueuesASlowRead_AndNothingForAFastOkRead()
    {
        var log = new SlowReadLog();
        var scope = NewScope();

        log.Offer(scope, ReadSurface.Mcp, "fast_tool", ReadOutcome.Ok, 40, null, null, null);
        Assert.False(log.TryRead(out _));

        log.Offer(scope, ReadSurface.Mcp, "slow_tool", ReadOutcome.Ok, 7_000, null, null, null);
        Assert.True(log.TryRead(out var record));
        Assert.Equal("slow_tool", record!.Route);
        Assert.Equal("mcp", record.Surface);
        Assert.Equal("ok", record.Outcome);
        Assert.Equal(7_000, record.TotalMs);
        Assert.Equal(DateTimeKind.Unspecified, record.ReadTimeUtc.Kind);
    }

    [Fact]
    public void Arguments_AreNormalised_ServerToIdAndHoursToAWindow_AndSecretKeysBlanked()
    {
        var arguments = new JsonObject
        {
            ["server_name"] = "some-host",
            ["hours_back"] = 48,
            ["top"] = 20,
            ["api_key"] = "do-not-store",
        };
        var started = new DateTime(2026, 10, 1, 12, 0, 0, DateTimeKind.Utc);

        var (json, truncated, start, end) = SlowReadLog.NormaliseArguments(arguments, 7, started);

        Assert.False(truncated);
        var parsed = JsonNode.Parse(json)!.AsObject();
        Assert.Equal(48, (int)parsed["hours_back"]!);
        Assert.Equal(20, (int)parsed["top"]!);
        Assert.Equal(7, (int)parsed["server_id"]!);
        Assert.Equal("[redacted]", (string)parsed["api_key"]!);
        Assert.False(parsed.ContainsKey("server_name"));
        Assert.DoesNotContain("some-host", json, StringComparison.Ordinal);
        Assert.Equal(new DateTime(2026, 10, 1, 12, 0, 0), end);
        Assert.Equal(new DateTime(2026, 9, 29, 12, 0, 0), start);
    }

    [Fact]
    public void Arguments_OverFourKilobytes_AreReplacedByATruncatedObject()
    {
        var arguments = new JsonObject { ["query"] = new string('x', 6_000), ["top"] = 5 };

        var (json, truncated, _, _) = SlowReadLog.NormaliseArguments(arguments, null, DateTime.UtcNow);

        Assert.True(truncated);
        Assert.True(Encoding.UTF8.GetByteCount(json) <= SlowReadLog.MaxArgumentsBytes);
        var parsed = JsonNode.Parse(json)!.AsObject();
        Assert.True((bool)parsed["truncated"]!);
        Assert.Contains("query", parsed["keys"]!.AsArray().Select(k => (string)k!));
    }

    [Fact]
    public void Statements_AreCappedAtFifty_AndSayMoreRan()
    {
        var log = new SlowReadLog();
        using var opened = ReadScope.Open(null);
        var scope = opened.Scope;
        scope.CaptureStatements = true;
        for (var i = 0; i < 60; i++)
        {
            scope.AddStatement("SELECT " + i, 1.25 + i, null);
        }

        log.Offer(scope, ReadSurface.Web, "route", ReadOutcome.Timeout, 100, null, "57014", null);
        Assert.True(log.TryRead(out var record));
        Assert.Equal(50, JsonNode.Parse(record!.StatementsJson)!.AsArray().Count);
        Assert.Equal(60, record.StatementCount);
        Assert.True(record.StatementsTruncated);
        Assert.Equal("57014", record.ErrorClass);
    }

    [Fact]
    public void TheQueue_HoldsAtMost256_DropsTheOldest_AndCountsTheDrops()
    {
        var log = new SlowReadLog();
        var scope = NewScope();
        for (var i = 0; i < SlowReadLog.Capacity + 10; i++)
        {
            log.Offer(scope, ReadSurface.Mcp, "r" + i, ReadOutcome.Error, 1, null, "X", null);
        }

        Assert.Equal(10, log.Dropped);
        Assert.True(log.TryRead(out var first));
        Assert.Equal("r10", first!.Route);
    }

    [Fact]
    public void ErrorClass_IsATypeNameOrSqlState_NeverMessageText()
    {
        Assert.Equal("InvalidOperationException", SlowReadLog.ErrorClassOf(new InvalidOperationException("secret text")));
        Assert.Equal("57014", SlowReadLog.ErrorClassOf(new PostgresException("canceling statement", "ERROR", "ERROR", "57014")));
    }

    private static DarlingSlowReadReader.SlowReadRow Row(long id, int statements)
    {
        var array = new JsonArray();
        for (var i = 1; i <= statements; i++)
        {
            array.Add(new JsonObject { ["ordinal"] = i, ["label"] = new string('q', 120), ["hash"] = "abcd1234", ["ms"] = 10.25 * i, ["rows"] = null });
        }

        return new DarlingSlowReadReader.SlowReadRow(
            id, new DateTime(2026, 10, 1, 0, 0, 0).AddSeconds(id), "mcp", "get_x", "timeout", 9_000, null, null, null, null,
            new JsonObject { ["top"] = 20 }, false, "raw", null, array, statements, false, "57014");
    }

    [Fact]
    public void TheResponse_FitsTheBudget_DropsRowsFromTheEnd_AndSaysSo()
    {
        var reads = Enumerable.Range(1, 100).Select(i => Row(i, 50)).ToList();
        var page = new DarlingSlowReadReader.Page(reads, new[] { new DarlingSlowReadReader.SummaryRow("mcp", "get_x", "timeout", 100) });

        var json = DarlingMcpSlowReadTools.BuildResponse(24, null, null, null, page, 100, fullDetail: true, McpResponseBudgetBytes);

        Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudgetBytes, "the payload is over the budget");
        var parsed = JsonNode.Parse(json)!.AsObject();
        Assert.True((bool)parsed["truncated"]!);
        var returned = (int)parsed["reads_returned"]!;
        Assert.InRange(returned, 1, 99);
        Assert.Equal(100, (int)parsed["reads_total"]!);
        /* Newest first: the dropped rows are the OLDEST, so the first row kept is the newest id. */
        Assert.Equal(returned, parsed["reads"]!.AsArray().Count);
        Assert.Contains("2026-10-01T00:01:40", (string)parsed["reads"]![0]!["read_time"]!, StringComparison.Ordinal);
    }

    [Fact]
    public void TheDefaultRowShowsTheFiveSlowestStatements_SlowestFirst_AndCountsTheOmitted()
    {
        var shaped = DarlingMcpSlowReadTools.ShapeStatements(Row(1, 8).Statements, fullDetail: false);
        Assert.Equal(5, shaped.Shown.Count);
        Assert.Equal(3, shaped.Omitted);
        var first = JsonNode.Parse(System.Text.Json.JsonSerializer.Serialize(shaped.Shown[0]))!;
        Assert.Equal(8, (int)first["ordinal"]!);
        Assert.Equal(82.0, (double)first["ms"]!);

        var full = DarlingMcpSlowReadTools.ShapeStatements(Row(1, 8).Statements, fullDetail: true);
        Assert.Equal(8, full.Shown.Count);
        Assert.Equal(0, full.Omitted);
    }

    [Fact]
    public async Task AWriteThatFails_IsSwallowed()
    {
        await using var dead = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Password=x;Timeout=1");
        var log = new SlowReadLog();
        var scope = NewScope();
        log.Offer(scope, ReadSurface.Mcp, "r", ReadOutcome.Error, 1, null, "X", null);
        Assert.True(log.TryRead(out var record));

        await SlowReadLog.StoreAsync(dead, record!, null);
    }

    private const int McpResponseBudgetBytes = 32 * 1024;
}
