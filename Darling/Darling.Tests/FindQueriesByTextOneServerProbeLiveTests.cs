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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test creates its own scratch database through ScratchPostgres and
   works entirely inside it, so it cannot race live collection. Each test builds the trigram index on its own tracker, never the process-wide one. */
/// <summary>
/// #8570: with the plan-cache text index <c>Ready</c>, the one-server scope of <c>find_queries_by_text</c> reads the matched digests while the Query Store
/// source runs, and each span reads the server's samples by them (<c>sample_read</c> <c>probe</c>, <c>matched_texts</c> the count). More texts than
/// <see cref="DarlingMcpFindQueriesByTextTools.ProbeDigestCap"/>, or a match read that runs out of its own timeout, take the span read
/// (<c>sample_read</c> <c>plain</c>). Either way the rows are the span read's rows in its order, and with the index not ready the reply carries
/// neither field. The fixture is <see cref="FindQueriesByTextIndexScopeLiveTests.OpenScratchAsync"/>: 3000 texts, server ix-a with a sample for
/// almost every text in Sales, server ix-b with two samples in Hr.
/// </summary>
[Collection("darling-config-env")]
public sealed class FindQueriesByTextOneServerProbeLiveTests
{
    /// <summary>One text: dbo.Orders_77.</summary>
    private const string RarePattern = "orders_77 where";

    /// <summary>111 texts: dbo.Orders_12, _120 to _129 and _1200 to _1299.</summary>
    private const string HundredPattern = "orders_12";

    /// <summary>Every one of the 3000 texts, more than the probe takes.</summary>
    private const string CommonPattern = "dbo.Orders_";

    private const int ExpectedCommonMatches = 3000;

    /// <summary>A budget, and a match-read timeout, no read here comes near: these tests are about which read serves the search and what it lists,
    /// so a slow or busy runner cannot stop a source part way or send the probe to the span read. The run-out test sets its own short match
    /// timeout on top of it.</summary>
    private static readonly DarlingMcpFindQueriesByTextTools.SourceBudget Roomy = new(60_000) { MatchMilliseconds = 60_000 };

    private static async Task<JsonElement> PlanCacheAsync(
        NpgsqlDataSource data, string pattern, string server, string? database, int limit, QueryTextTrigramIndexTracker tracker, CancellationToken ct,
        DarlingMcpFindQueriesByTextTools.SourceBudget? budget = null) =>
        (await ReplyAsync(data, pattern, server, database, limit, tracker, ct, budget)).GetProperty("sources").GetProperty("plan_cache");

    private static async Task<JsonElement> ReplyAsync(
        NpgsqlDataSource data, string pattern, string server, string? database, int limit, QueryTextTrigramIndexTracker tracker, CancellationToken ct,
        DarlingMcpFindQueriesByTextTools.SourceBudget? budget = null)
    {
        var reply = JsonDocument.Parse(await DarlingMcpFindQueriesByTextTools.FindQueriesByText(
            data, pattern, server, database, limit, McpDataDetail.Full,
            Roomy, budget ?? Roomy, ct, tracker)).RootElement;
        Assert.Equal("one_server", reply.GetProperty("scope").GetString());
        return reply;
    }

    /// <summary>The rows of the plan-cache source, in reply order, as <c>query_hash@database</c>.</summary>
    private static string[] Keys(JsonElement planCache) =>
        planCache.GetProperty("rows").EnumerateArray()
            .Select(r => r.GetProperty("query_hash").GetString() + "@" + r.GetProperty("database_name").GetString())
            .ToArray();

    /// <summary>A budget that records, for every sample statement the search runs, the type of its third parameter: the matched digests
    /// (<c>bytea[]</c>) on the probe, the pattern (<c>text</c>) on the span read.</summary>
    private static DarlingMcpFindQueriesByTextTools.SourceBudget Recording(List<NpgsqlDbType> bound, DarlingMcpFindQueriesByTextTools.SourceBudget? from = null) =>
        (from ?? Roomy) with { StatementBound = (_, p) => bound.Add(p[2].NpgsqlDbType) };

    private const NpgsqlDbType Digests = NpgsqlDbType.Array | NpgsqlDbType.Bytea;

    /// <summary>The probe and the span read list the same rows, field for field, in the same order, with the same status and page.</summary>
    private static void AssertSameRows(JsonElement probe, JsonElement plain)
    {
        Assert.Equal(plain.GetProperty("status").GetString(), probe.GetProperty("status").GetString());
        Assert.Equal(Keys(plain), Keys(probe));
        Assert.Equal(plain.GetProperty("rows").GetRawText(), probe.GetProperty("rows").GetRawText());
        Assert.Equal(plain.GetProperty("returned").GetInt32(), probe.GetProperty("returned").GetInt32());
        Assert.Equal(plain.GetProperty("truncated").GetBoolean(), probe.GetProperty("truncated").GetBoolean());
    }

    [Fact]
    public async Task IndexReady_ARarePattern_ReadsTheMatchedTextsFirst()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await FindQueriesByTextIndexScopeLiveTests.OpenScratchAsync(ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        var tracker = await FindQueriesByTextIndexScopeLiveTests.BuildIndexAsync(data, ct);
        var bound = new List<NpgsqlDbType>();

        var planCache = await PlanCacheAsync(data, RarePattern, "ix-a", null, 20, tracker, ct, Recording(bound));

        Assert.Equal("ok", planCache.GetProperty("status").GetString());
        Assert.Equal(DarlingMcpFindQueriesByTextTools.SampleReadProbe, planCache.GetProperty("sample_read").GetString());
        Assert.Equal(1, planCache.GetProperty("matched_texts").GetInt32());
        Assert.Equal(["0xA001@Sales"], Keys(planCache));
        Assert.NotEmpty(bound);
        Assert.All(bound, type => Assert.Equal(Digests, type));
    }

    [Theory]
    [InlineData(RarePattern, "ix-a", null, 20)]
    [InlineData(RarePattern, "ix-b", null, 20)]
    [InlineData("orders_123", "ix-a", null, 50)]
    [InlineData(HundredPattern, "ix-a", null, 50)]
    [InlineData(HundredPattern, "ix-a", "Sales", 50)]
    [InlineData(HundredPattern, "ix-b", null, 20)]
    [InlineData(HundredPattern, "ix-a", "Hr", 20)]
    public async Task IndexReady_TheProbeListsWhatTheSpanReadLists(string pattern, string server, string? database, int limit)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await FindQueriesByTextIndexScopeLiveTests.OpenScratchAsync(ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        var tracker = await FindQueriesByTextIndexScopeLiveTests.BuildIndexAsync(data, ct);

        var probe = await PlanCacheAsync(data, pattern, server, database, limit, tracker, ct);
        var plain = await PlanCacheAsync(data, pattern, server, database, limit, new QueryTextTrigramIndexTracker(), ct);

        Assert.Equal(DarlingMcpFindQueriesByTextTools.SampleReadProbe, probe.GetProperty("sample_read").GetString());
        AssertSameRows(probe, plain);
    }

    /// <summary>Spans are read newest first, a statement an earlier span found is not listed again from an older one, and the page stops at the
    /// limit across spans, as on the span read.</summary>
    [Fact]
    public async Task IndexReady_SpansNewestFirst_AStatementANewerSpanFoundIsNotListedAgain()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await FindQueriesByTextIndexScopeLiveTests.OpenScratchAsync(ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using (var c = await data.OpenConnectionAsync(ct))
        {
            async Task Stat(string database, string hash, TimeSpan age) =>
                await DarlingMcpTestData.ExecAsync(c, ct,
                    @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_text_digest)
                      VALUES ($1,$2,1,'ix-a',$3,$4,sha256('77'::text::bytea))",
                    CollectionIdGenerator.Next(), LiveClock.AnchoredEnd() - age, database, hash);

            await Stat("Sales", "0xA001", TimeSpan.FromDays(2));
            await Stat("Sales", "0xA00F", TimeSpan.FromDays(2));
            await Stat("Ops", "0xA00E", TimeSpan.FromDays(2.5));
        }

        var tracker = await FindQueriesByTextIndexScopeLiveTests.BuildIndexAsync(data, ct);
        var bound = new List<NpgsqlDbType>();

        var probe = await PlanCacheAsync(data, RarePattern, "ix-a", null, 20, tracker, ct, Recording(bound));
        var plain = await PlanCacheAsync(data, RarePattern, "ix-a", null, 20, new QueryTextTrigramIndexTracker(), ct);

        Assert.Equal(["0xA001@Sales", "0xA00F@Sales", "0xA00E@Ops"], Keys(probe));
        AssertSameRows(probe, plain);
        Assert.True(bound.Count > 1, "the rows sit in more than one span, so the search ran more than one sample statement");
        Assert.All(bound, type => Assert.Equal(Digests, type));

        var probePage = await PlanCacheAsync(data, RarePattern, "ix-a", null, 2, tracker, ct);
        var plainPage = await PlanCacheAsync(data, RarePattern, "ix-a", null, 2, new QueryTextTrigramIndexTracker(), ct);

        Assert.Equal(["0xA001@Sales", "0xA00F@Sales"], Keys(probePage));
        Assert.True(probePage.GetProperty("truncated").GetBoolean());
        AssertSameRows(probePage, plainPage);
    }

    [Fact]
    public async Task IndexReady_MoreTextsThanTheProbeTakesMatch_TakesTheSpanRead()
    {
        Assert.True(ExpectedCommonMatches > DarlingMcpFindQueriesByTextTools.ProbeDigestCap, "the common pattern must match more texts than the probe takes");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await FindQueriesByTextIndexScopeLiveTests.OpenScratchAsync(ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        var tracker = await FindQueriesByTextIndexScopeLiveTests.BuildIndexAsync(data, ct);
        var bound = new List<NpgsqlDbType>();

        var planCache = await PlanCacheAsync(data, CommonPattern, "ix-a", null, 20, tracker, ct, Recording(bound));
        var plain = await PlanCacheAsync(data, CommonPattern, "ix-a", null, 20, new QueryTextTrigramIndexTracker(), ct);

        Assert.Equal(DarlingMcpFindQueriesByTextTools.SampleReadPlain, planCache.GetProperty("sample_read").GetString());
        Assert.Equal(JsonValueKind.Null, planCache.GetProperty("matched_texts").ValueKind);
        Assert.Equal(20, planCache.GetProperty("returned").GetInt32());
        AssertSameRows(planCache, plain);
        Assert.NotEmpty(bound);
        Assert.All(bound, type => Assert.Equal(NpgsqlDbType.Text, type));
    }

    /// <summary>The match read runs out of its own timeout (a sleep inside it, past a 100 ms timeout): the search takes the span read.</summary>
    [Fact]
    public async Task IndexReady_TheMatchReadRunsOut_TakesTheSpanRead()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await FindQueriesByTextIndexScopeLiveTests.OpenScratchAsync(ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        var tracker = await FindQueriesByTextIndexScopeLiveTests.BuildIndexAsync(data, ct);
        var bound = new List<NpgsqlDbType>();
        var slowMatch = Roomy with { MatchMilliseconds = 100, MatchTestDelaySeconds = 5 };

        var planCache = await PlanCacheAsync(data, RarePattern, "ix-a", null, 20, tracker, ct, Recording(bound, slowMatch));
        var plain = await PlanCacheAsync(data, RarePattern, "ix-a", null, 20, new QueryTextTrigramIndexTracker(), ct);

        Assert.True(planCache.GetProperty("status").GetString() == "ok", planCache.GetProperty("message").ToString());
        Assert.Equal(DarlingMcpFindQueriesByTextTools.SampleReadPlain, planCache.GetProperty("sample_read").GetString());
        Assert.Equal(JsonValueKind.Null, planCache.GetProperty("matched_texts").ValueKind);
        Assert.Equal(["0xA001@Sales"], Keys(planCache));
        AssertSameRows(planCache, plain);
        Assert.All(bound, type => Assert.Equal(NpgsqlDbType.Text, type));
    }

    /// <summary>#9150: a match read that is slow but inside the source's budget (a sleep of 1.5 s inside it, past the 1 s the one-server search once
    /// gave it) still serves the search by the digests, as the all-servers search's match read does. The budget is the tool's own default for the
    /// match read; the rest of it is roomy so the sample read cannot run out.</summary>
    [Fact]
    public async Task IndexReady_ASlowButInBudgetMatchRead_StillTakesTheProbe()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await FindQueriesByTextIndexScopeLiveTests.OpenScratchAsync(ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        var tracker = await FindQueriesByTextIndexScopeLiveTests.BuildIndexAsync(data, ct);
        var slowMatch = new DarlingMcpFindQueriesByTextTools.SourceBudget(60_000) { MatchTestDelaySeconds = 1.5 };

        var planCache = await PlanCacheAsync(data, RarePattern, "ix-a", null, 20, tracker, ct, slowMatch);

        Assert.True(planCache.GetProperty("status").GetString() == "ok", planCache.GetProperty("message").ToString());
        Assert.Equal(DarlingMcpFindQueriesByTextTools.SampleReadProbe, planCache.GetProperty("sample_read").GetString());
        Assert.Equal(1, planCache.GetProperty("matched_texts").GetInt32());
        Assert.Equal(["0xA001@Sales"], Keys(planCache));
    }

    /// <summary>#9150: a finished one-server search says its page is the exact answer for the whole raw window of the server it was asked about.</summary>
    [Fact]
    public async Task AFinishedSearch_SaysItSearchedTheServersWholeWindow()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await FindQueriesByTextIndexScopeLiveTests.OpenScratchAsync(ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);

        var searched = (await PlanCacheAsync(data, RarePattern, "ix-a", null, 20, new QueryTextTrigramIndexTracker(), ct)).GetProperty("searched");

        Assert.Equal(["ix-a"], searched.GetProperty("servers").EnumerateArray().Select(s => s.GetString()).ToArray());
        Assert.True(searched.GetProperty("complete").GetBoolean());
        Assert.Equal(searched.GetProperty("window_from").GetString(), searched.GetProperty("searched_from").GetString());
        Assert.Equal(searched.GetProperty("window_to").GetString(), searched.GetProperty("searched_to").GetString());
    }

    /// <summary>The match read runs while the Query Store source does: the Query Store statement is held until the match read has started, which it
    /// can do only if it does not wait for that source to finish. The hold gives up after 30 s, so a match read that waits fails the test rather
    /// than hanging it.</summary>
    [Fact]
    public async Task IndexReady_TheMatchReadRunsWhileTheQueryStoreSourceDoes()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await FindQueriesByTextIndexScopeLiveTests.OpenScratchAsync(ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        var tracker = await FindQueriesByTextIndexScopeLiveTests.BuildIndexAsync(data, ct);
        var matchStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var matchStartedFirst = false;
        var queryStoreBudget = Roomy with
        {
            BeforeStatement = async (_, _, token) =>
                matchStartedFirst = await Task.WhenAny(matchStarted.Task, Task.Delay(TimeSpan.FromSeconds(30), token)) == matchStarted.Task,
        };
        var planCacheBudget = Roomy with { MatchStarting = () => matchStarted.TrySetResult() };

        var reply = JsonDocument.Parse(await DarlingMcpFindQueriesByTextTools.FindQueriesByText(
            data, RarePattern, "ix-a", null, 20, McpDataDetail.Full, queryStoreBudget, planCacheBudget, ct, tracker)).RootElement;
        var planCache = reply.GetProperty("sources").GetProperty("plan_cache");

        Assert.True(matchStartedFirst, "the Query Store statement ran before the match read started, so the match read waited for that source");
        Assert.Equal("ok", planCache.GetProperty("status").GetString());
        Assert.Equal(DarlingMcpFindQueriesByTextTools.SampleReadProbe, planCache.GetProperty("sample_read").GetString());
        Assert.Equal(1, planCache.GetProperty("matched_texts").GetInt32());
        Assert.Equal(["0xA001@Sales"], Keys(planCache));
    }

    [Fact]
    public async Task IndexReady_NothingMatches_IsEmptyAndRunsNoSampleStatement()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await FindQueriesByTextIndexScopeLiveTests.OpenScratchAsync(ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        var tracker = await FindQueriesByTextIndexScopeLiveTests.BuildIndexAsync(data, ct);
        var bound = new List<NpgsqlDbType>();

        var reply = await ReplyAsync(data, "no_such_table_anywhere", "ix-a", null, 20, tracker, ct, Recording(bound));
        var planCache = reply.GetProperty("sources").GetProperty("plan_cache");

        Assert.Equal("empty", planCache.GetProperty("status").GetString());
        Assert.Equal(DarlingMcpFindQueriesByTextTools.SampleReadProbe, planCache.GetProperty("sample_read").GetString());
        Assert.Equal(0, planCache.GetProperty("matched_texts").GetInt32());
        Assert.Equal(0, planCache.GetProperty("returned").GetInt32());
        Assert.Empty(bound);
        Assert.Equal("empty", reply.GetProperty("sources").GetProperty("query_store").GetProperty("status").GetString());
    }

    /// <summary>The option unset, off, or on with the index not built yet: the span read serves the search and the reply carries neither field.</summary>
    [Theory]
    [InlineData("unset")]
    [InlineData("off")]
    [InlineData("on, not built")]
    public async Task IndexNotReady_TheReplyCarriesNeitherField(string option)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await FindQueriesByTextIndexScopeLiveTests.OpenScratchAsync(ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        var tracker = new QueryTextTrigramIndexTracker();
        if (option != "unset")
        {
            tracker.Configure(option != "off");
        }

        var bound = new List<NpgsqlDbType>();
        var planCache = await PlanCacheAsync(data, RarePattern, "ix-a", null, 20, tracker, ct, Recording(bound));

        Assert.Equal("ok", planCache.GetProperty("status").GetString());
        Assert.Equal(["0xA001@Sales"], Keys(planCache));
        Assert.False(planCache.TryGetProperty("sample_read", out _));
        Assert.False(planCache.TryGetProperty("matched_texts", out _));
        Assert.All(bound, type => Assert.Equal(NpgsqlDbType.Text, type));
    }
}
