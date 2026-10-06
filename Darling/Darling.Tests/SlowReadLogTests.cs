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
        var log = new SlowReadLog();
        Assert.Equal(5_000, log.ThresholdMs);

        Assert.True(log.ShouldRecord(ReadOutcome.Ok, 5_000));
        Assert.False(log.ShouldRecord(ReadOutcome.Ok, 4_999));
        Assert.True(log.ShouldRecord(ReadOutcome.Timeout, 10));
        Assert.True(log.ShouldRecord(ReadOutcome.Error, 10));
        Assert.True(log.ShouldRecord(ReadOutcome.Limit, 10));
        Assert.False(log.ShouldRecord(ReadOutcome.Cancelled, 10));
        Assert.False(log.ShouldRecord(ReadOutcome.FallbackRaw, 10));
        Assert.False(log.ShouldRecord(ReadOutcome.GateFailed, 10));
        Assert.True(log.ShouldRecord(ReadOutcome.Cancelled, 6_000));
        Assert.True(log.ShouldRecord(ReadOutcome.FallbackRaw, 6_000));
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
        /* Strings over 128 characters are omitted, so the size bound is reached by many short keys. */
        var arguments = new JsonObject { ["top"] = 5 };
        for (var i = 0; i < 300; i++)
        {
            arguments["query_" + i.ToString("D3", System.Globalization.CultureInfo.InvariantCulture)] = new string('x', 100);
        }

        var (json, truncated, _, _) = SlowReadLog.NormaliseArguments(arguments, null, DateTime.UtcNow);

        Assert.True(truncated);
        Assert.True(Encoding.UTF8.GetByteCount(json) <= SlowReadLog.MaxArgumentsBytes);
        var parsed = JsonNode.Parse(json)!.AsObject();
        Assert.True((bool)parsed["truncated"]!);
        Assert.Contains("query_000", parsed["keys"]!.AsArray().Select(k => (string)k!));
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

    private static JsonObject Stored(JsonObject arguments, out string json)
    {
        (json, _, _, _) = SlowReadLog.NormaliseArguments(arguments, null, DateTime.UtcNow);
        return JsonNode.Parse(json)!.AsObject();
    }

    [Fact]
    public void AServerBatchArgument_CarryingAPassword_IsNeverStored()
    {
        var arguments = new JsonObject
        {
            ["servers_json"] = "[{\"server_name\":\"x\",\"password\":\"Hunter2!\"}]",
        };

        var stored = Stored(arguments, out var json);

        Assert.DoesNotContain("Hunter2!", json, StringComparison.Ordinal);
        Assert.DoesNotContain("password\":\"", json, StringComparison.Ordinal);
        Assert.Equal(SlowReadLog.OmittedMarker, (string)stored["servers_json"]!);
    }

    [Fact]
    public void ASettingsJsonArgument_CarryingAWebhookSecret_IsOmitted()
    {
        var arguments = new JsonObject
        {
            ["settings_json"] = "{\"smtp\":{\"notes\":\"x\"},\"hook\":\"https://hooks.example.test/T000/B000/s3cr3tvalue\"}",
        };

        var stored = Stored(arguments, out var json);

        Assert.DoesNotContain("s3cr3tvalue", json, StringComparison.Ordinal);
        Assert.Equal(SlowReadLog.OmittedMarker, (string)stored["settings_json"]!);
    }

    [Fact]
    public void ANestedSecretKey_InAComposeBody_IsRedactedAtAnyDepth()
    {
        var body = JsonNode.Parse("{\"panel\":{\"options\":{\"api_key\":\"abc123\",\"top\":5}},\"list\":[{\"Password\":\"p\"}]}")!.AsObject();

        var stored = Stored(body, out var json);

        Assert.DoesNotContain("abc123", json, StringComparison.Ordinal);
        Assert.Equal(SlowReadLog.RedactedMarker, (string)stored["panel"]!["options"]!["api_key"]!);
        Assert.Equal(5, (int)stored["panel"]!["options"]!["top"]!);
        Assert.Equal(SlowReadLog.RedactedMarker, (string)stored["list"]![0]!["Password"]!);
    }

    [Fact]
    public void AWebQueryString_RedactsTheToken_AndKeepsTheScalars()
    {
        var query = new JsonObject { ["token"] = "abc", ["hours_back"] = "48" };

        var stored = Stored(query, out _);

        Assert.Equal(SlowReadLog.RedactedMarker, (string)stored["token"]!);
        Assert.Equal("48", (string)stored["hours_back"]!);
    }

    [Fact]
    public void NumbersBooleansAndShortStrings_AreKept()
    {
        var stored = Stored(new JsonObject { ["hours_back"] = 48, ["top"] = 20, ["flag"] = true, ["view"] = "waits" }, out _);

        Assert.Equal(48, (int)stored["hours_back"]!);
        Assert.Equal(20, (int)stored["top"]!);
        Assert.True((bool)stored["flag"]!);
        Assert.Equal("waits", (string)stored["view"]!);
    }

    [Fact]
    public void AnyNonJsonString_OfMoreThan128Characters_IsOmitted_AndExactly128IsKept()
    {
        var stored = Stored(new JsonObject { ["long"] = new string('a', 129), ["edge"] = new string('b', 128) }, out _);

        Assert.Equal(SlowReadLog.OmittedMarker, (string)stored["long"]!);
        Assert.Equal(128, ((string)stored["edge"]!).Length);
    }

    [Fact]
    public void AnyShortStringWithABrace_OrBracket_IsOmitted_ParsedOrNot()
    {
        var stored = Stored(new JsonObject { ["a"] = "{\"k\":1}", ["b"] = "[1,2]", ["c"] = "[not json", ["d"] = "{also not" }, out _);

        Assert.Equal(SlowReadLog.OmittedMarker, (string)stored["a"]!);
        Assert.Equal(SlowReadLog.OmittedMarker, (string)stored["b"]!);
        Assert.Equal(SlowReadLog.OmittedMarker, (string)stored["c"]!);
        Assert.Equal(SlowReadLog.OmittedMarker, (string)stored["d"]!);
    }

    [Fact]
    public void Nesting_StopsAtDepthEight_AndAnArrayAtFiftyElements()
    {
        JsonNode inner = new JsonObject { ["leaf"] = 1 };
        for (var i = 0; i < 10; i++)
        {
            inner = new JsonObject { ["n"] = inner };
        }

        var many = new JsonArray();
        for (var i = 0; i < 60; i++)
        {
            many.Add(i);
        }

        var stored = Stored(new JsonObject { ["deep"] = inner, ["many"] = many }, out var json);

        Assert.Contains("\"[omitted]\"", json, StringComparison.Ordinal);
        Assert.DoesNotContain("leaf", json, StringComparison.Ordinal);
        var array = stored["many"]!.AsArray();
        Assert.Equal(51, array.Count);
        Assert.Equal("[omitted 10 more]", (string)array[50]!);
    }

    [Theory]
    [InlineData("password")]
    [InlineData("passwd")]
    [InlineData("pwd")]
    [InlineData("client_secret")]
    [InlineData("access_token")]
    [InlineData("apikey")]
    [InlineData("api_key")]
    [InlineData("license_key")]
    [InlineData("credential")]
    [InlineData("connectionstring")]
    [InlineData("connection_string")]
    [InlineData("authorization")]
    public void EverySecretFragment_IsRedacted(string key)
    {
        var stored = Stored(new JsonObject { [key] = "v", ["x"] = new JsonObject { [key.ToUpperInvariant()] = "v" } }, out _);

        Assert.Equal(SlowReadLog.RedactedMarker, (string)stored[key]!);
        Assert.Equal(SlowReadLog.RedactedMarker, (string)stored["x"]![key.ToUpperInvariant()]!);
    }

    [Fact]
    public void TheStatementCapture_ReadsOnlyTheQueryTextAndReturnedRowsTags()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "ReadStatementCapture.cs");
        var tagKeys = System.Text.RegularExpressions.Regex.Matches(source, "\"(db\\.[A-Za-z0-9_.]+)\"")
            .Select(m => m.Groups[1].Value).Distinct().OrderBy(k => k, StringComparer.Ordinal).ToArray();

        Assert.Equal(new[] { "db.query.text", "db.response.returned_rows" }, tagKeys);
        Assert.DoesNotContain("data_source", source, StringComparison.Ordinal);
    }

    [Fact]
    public void ABuiltRecord_CarriesTheRowCountTheReadNoted()
    {
        using var opened = ReadScope.Open(null);
        ReadScope.NoteRows(1234);

        var record = SlowReadLog.Build(opened.Scope, ReadSurface.Mcp, "r", ReadOutcome.Ok, 9_000, null, null, DateTime.UtcNow);
        Assert.Equal(1234L, record.RowCount);

        using var none = ReadScope.Open(null);
        Assert.Null(SlowReadLog.Build(none.Scope, ReadSurface.Mcp, "r", ReadOutcome.Ok, 9_000, null, null, DateTime.UtcNow).RowCount);
    }

    [Fact]
    public void ThePurge_RunsFirst_ThenAtMostOncePerMinuteOrPerHundredInserts()
    {
        var log = new SlowReadLog();
        var t0 = new DateTime(2026, 10, 1, 12, 0, 0, DateTimeKind.Utc);

        Assert.True(log.PurgeDue(t0));
        for (var i = 0; i < 50; i++)
        {
            Assert.False(log.PurgeDue(t0.AddSeconds(1)));
        }

        Assert.True(log.PurgeDue(t0.AddSeconds(61)), "a minute on");
        for (var i = 1; i < SlowReadLog.PurgeEveryInserts; i++)
        {
            Assert.False(log.PurgeDue(t0.AddSeconds(62)));
        }

        Assert.True(log.PurgeDue(t0.AddSeconds(62)), "the hundredth insert");
    }

    [Fact]
    public void TheThreshold_IsInjectedPerInstance()
    {
        var instant = new SlowReadLog(0);
        var normal = new SlowReadLog();

        Assert.True(instant.ShouldRecord(ReadOutcome.Ok, 0));
        Assert.False(normal.ShouldRecord(ReadOutcome.Ok, 0));
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

        await log.StoreAsync(dead, record!, null);
    }

    [Fact]
    public void ABomPrefixedOrTrailingCommaJsonString_IsOmitted()
    {
        var stored = Stored(new JsonObject { ["a"] = "\uFEFF{\"password\":\"x\"}", ["b"] = "{\"password\":\"x\",}", ["c"] = "/*c*/{\"a\":1}" }, out _);

        Assert.Equal(SlowReadLog.OmittedMarker, (string)stored["a"]!);
        Assert.Equal(SlowReadLog.OmittedMarker, (string)stored["b"]!);
        Assert.Equal(SlowReadLog.OmittedMarker, (string)stored["c"]!);
    }

    [Fact]
    public void AUrlCredentialOrPasswordBearingString_IsOmitted_AndAPlainUrlIsKept()
    {
        var stored = Stored(new JsonObject { ["a"] = "https://u:p@h/x", ["b"] = "Server=a;Password=b", ["c"] = "Server=a;PWD = b", ["d"] = "https://h/x" }, out _);

        Assert.Equal(SlowReadLog.OmittedMarker, (string)stored["a"]!);
        Assert.Equal(SlowReadLog.OmittedMarker, (string)stored["b"]!);
        Assert.Equal(SlowReadLog.OmittedMarker, (string)stored["c"]!);
        Assert.Equal("https://h/x", (string)stored["d"]!);
    }

    [Fact]
    public void DedupKey_IsKept_WhileApiKeyIsRedacted_AndOrdinaryArgumentsSurvive()
    {
        var stored = Stored(new JsonObject { ["dedup_key"] = "abc", ["api_key"] = "abc", ["hours_back"] = 4, ["top"] = 5, ["server_name"] = "s" }, out _);

        Assert.Equal("abc", (string)stored["dedup_key"]!);
        Assert.Equal(SlowReadLog.RedactedMarker, (string)stored["api_key"]!);
        Assert.Equal(4, (int)stored["hours_back"]!);
        Assert.Equal(5, (int)stored["top"]!);
    }

    [Fact]
    public void AWebQueryKeyWithOddCharacters_IsDroppedAndCounted_AndALongKeyIsCappedAt64()
    {
        var args = DarlingWebEndpoints.WebQueryArguments(new[]
        {
            new KeyValuePair<string, string?>("bad key!", "v"),
            new KeyValuePair<string, string?>(new string('k', 100), "v"),
            new KeyValuePair<string, string?>("top", "5"),
        });

        Assert.False(args.ContainsKey("bad key!"));
        Assert.Equal(1, (int)args["_dropped_keys"]!);
        Assert.True(args.ContainsKey(new string('k', 64)));
        Assert.False(args.ContainsKey(new string('k', 100)));
        Assert.Equal("5", (string)args["top"]!);
        Assert.False(DarlingWebEndpoints.WebQueryArguments(new[] { new KeyValuePair<string, string?>("top", "5") }).ContainsKey("_dropped_keys"));
    }

    /// <summary>A repeated key is recorded as a JSON array of its values, in the order sent; a key sent once stays a
    /// plain string, and the 64-character key rule still applies to a repeated key (#5245).</summary>
    [Fact]
    public void ARepeatedWebQueryKey_KeepsEveryValueAsAnArray_AndASingleKeyIsUnchanged()
    {
        var args = DarlingWebEndpoints.WebQueryArguments(new[]
        {
            new KeyValuePair<string, string?>("database_name", "A"),
            new KeyValuePair<string, string?>("top", "5"),
            new KeyValuePair<string, string?>("database_name", "B"),
            new KeyValuePair<string, string?>("server", ""),
            new KeyValuePair<string, string?>("server", "S2"),
            new KeyValuePair<string, string?>(new string('k', 100), "x"),
            new KeyValuePair<string, string?>(new string('k', 100), "y"),
        });

        var databases = Assert.IsType<JsonArray>(args["database_name"]);
        Assert.Equal(new[] { "A", "B" }, databases.Select(v => (string?)v).ToArray());
        Assert.Equal(new[] { "", "S2" }, Assert.IsType<JsonArray>(args["server"]).Select(v => (string?)v).ToArray());
        Assert.Equal(new[] { "x", "y" }, Assert.IsType<JsonArray>(args[new string('k', 64)]).Select(v => (string?)v).ToArray());
        Assert.Equal("5", Assert.IsAssignableFrom<JsonValue>(args["top"]).GetValue<string>());
    }

    [Fact]
    public void AnUnknownComposeSource_RecordsComposeUnknown_AndAKnownOneKeepsItsName()
    {
        Assert.Equal("compose:unknown", DarlingWebEndpoints.ComposeRouteLabel(new JsonObject { ["panel"] = new JsonObject { ["source"] = new string('x', 500) } }));
        Assert.Equal("compose:wait_stats", DarlingWebEndpoints.ComposeRouteLabel(new JsonObject { ["panel"] = new JsonObject { ["source"] = "wait_stats" } }));
    }

    private const int McpResponseBudgetBytes = 32 * 1024;
}
