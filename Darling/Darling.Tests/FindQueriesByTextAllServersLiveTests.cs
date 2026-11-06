/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test creates its own scratch database through ScratchPostgres and
   works entirely inside it, so it cannot race live collection. */
/// <summary>
/// #7674 part B, the all-servers scope of <c>find_queries_by_text</c>: served through the trigram index only when it is ready, refused with the
/// option's name and the state in every other state. The one-server scope is in <see cref="FindQueriesByTextIndexScopeLiveTests"/>.
/// </summary>
public sealed class FindQueriesByTextAllServersLiveTests
{
    private static Task<string> RunTool(NpgsqlDataSource? data, string pattern, string? server, string? database, QueryTextTrigramIndexTracker? tracker, CancellationToken ct) =>
        DarlingMcpFindQueriesByTextTools.FindQueriesByText(
            data!, pattern, server, database, 20, McpDataDetail.Full,
            DarlingMcpFindQueriesByTextTools.SourceBudget.Default, DarlingMcpFindQueriesByTextTools.SourceBudget.Default, ct, tracker);


    /// <summary>The all-servers statement keeps the bare column, so with sequential scans off the planner reaches the text through the trigram index.
    /// Without this the narrow-scope check above would pass for any statement, including one the index could never serve.</summary>
    [Fact]
    public async Task AllServersSql_UsesTheTrigramIndex_WhenItExists()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await FindQueriesByTextIndexScopeLiveTests.OpenScratchAsync(ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        await FindQueriesByTextIndexScopeLiveTests.BuildIndexAsync(data, ct);

        var plan = await FindQueriesByTextIndexScopeLiveTests.ExplainAsync(data, DarlingMcpFindQueriesByTextTools.PlanCacheAllServersSql, null, "orders_77 where", withServer: false, ct);

        Assert.Contains(FindQueriesByTextIndexScopeLiveTests.IndexShortName, plan, StringComparison.Ordinal);
    }

    [Fact]
    public async Task NoServerNamed_WithAReadyIndex_SearchesEveryServersPlanCache_AndTheOneServerScopeStillAnswers()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await FindQueriesByTextIndexScopeLiveTests.OpenScratchAsync(ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        var tracker = await FindQueriesByTextIndexScopeLiveTests.BuildIndexAsync(data, ct);

        var all = JsonDocument.Parse(await RunTool(data, "orders_77 where", null, null, tracker, ct)).RootElement;
        Assert.Equal("all_servers", all.GetProperty("scope").GetString());
        var planCache = all.GetProperty("sources").GetProperty("plan_cache");
        Assert.Equal("ok", planCache.GetProperty("status").GetString());
        var rows = planCache.GetProperty("rows").EnumerateArray().ToArray();
        Assert.Equal(new[] { "ix-a", "ix-b" }, rows.Select(r => r.GetProperty("server").GetString()).OrderBy(x => x, StringComparer.Ordinal).ToArray());
        Assert.Equal(new[] { "0xA001", "0xB002" }, rows.Select(r => r.GetProperty("query_hash").GetString()).OrderBy(x => x, StringComparer.Ordinal).ToArray());
        Assert.All(rows, r => Assert.Contains("Orders_77", r.GetProperty("excerpt").GetString(), StringComparison.Ordinal));
        var queryStore = all.GetProperty("sources").GetProperty("query_store");
        Assert.Equal("not_searched", queryStore.GetProperty("status").GetString());
        Assert.Contains("server_name", queryStore.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Empty(queryStore.GetProperty("rows").EnumerateArray());

        var only = JsonDocument.Parse(await RunTool(data, "orders_1234 where", null, null, tracker, ct)).RootElement;
        var onlyRows = only.GetProperty("sources").GetProperty("plan_cache").GetProperty("rows").EnumerateArray().ToArray();
        Assert.Equal("ix-b", Assert.Single(onlyRows).GetProperty("server").GetString());

        var one = JsonDocument.Parse(await RunTool(data, "orders_77 where", "ix-a", null, tracker, ct)).RootElement;
        Assert.Equal("one_server", one.GetProperty("scope").GetString());
        var oneRows = one.GetProperty("sources").GetProperty("plan_cache").GetProperty("rows").EnumerateArray().ToArray();
        Assert.Equal("0xA001", Assert.Single(oneRows).GetProperty("query_hash").GetString());
    }

    [Fact]
    public async Task NoServerNamed_WhenTheIndexIsPending_OrIsBeingDropped_IsRefusedWithThatState()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await FindQueriesByTextIndexScopeLiveTests.OpenScratchAsync(ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);

        var pending = new QueryTextTrigramIndexTracker();
        pending.Configure(true);
        var notBuilt = JsonDocument.Parse(await RunTool(data, "orders_77 where", null, null, pending, ct)).RootElement;
        Assert.Equal("invalid", notBuilt.GetProperty("status").GetString());
        Assert.Contains("state: pending", notBuilt.GetProperty("message").GetString(), StringComparison.Ordinal);

        var tracker = await FindQueriesByTextIndexScopeLiveTests.BuildIndexAsync(data, ct);
        tracker.Configure(false);
        var dropping = JsonDocument.Parse(await RunTool(data, "orders_77 where", null, null, tracker, ct)).RootElement;
        Assert.Equal("invalid", dropping.GetProperty("status").GetString());
        Assert.False(dropping.TryGetProperty("sources", out _), "an index about to be dropped served an all-servers search");
        Assert.Contains("being dropped", dropping.GetProperty("message").GetString(), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(TrigramIndexStatus.Off, "state: off")]
    [InlineData(TrigramIndexStatus.DropPending, "being dropped")]
    [InlineData(TrigramIndexStatus.Dropping, "being dropped")]
    [InlineData(TrigramIndexStatus.Pending, "state: pending")]
    [InlineData(TrigramIndexStatus.Building, "state: building")]
    [InlineData(TrigramIndexStatus.Failed, "state: failed")]
    [InlineData(TrigramIndexStatus.WaitingOnExtension, "waiting on the pg_trgm extension")]
    public void TheRefusal_NamesTheOptionAndEachState(TrigramIndexStatus status, string word)
    {
        var health = new TrigramIndexHealth(status, false, false, null, null, false, null, false, true, 0, null, null, null, null,
            status == TrigramIndexStatus.WaitingOnExtension ? QueryTextTrigramIndex.OperatorStatement : null);

        var sentence = DarlingMcpFindQueriesByTextTools.AllServersRefusal(health);

        Assert.Contains("queryTextTrigramIndexEnabled", sentence, StringComparison.Ordinal);
        Assert.Contains(word, sentence, StringComparison.Ordinal);
        Assert.Contains("server_name", sentence, StringComparison.Ordinal);
        if (status == TrigramIndexStatus.WaitingOnExtension)
        {
            Assert.Contains(QueryTextTrigramIndex.OperatorStatement, sentence, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheRefusal_WhenTheStateCannotBeRead_SaysSo()
    {
        var sentence = DarlingMcpFindQueriesByTextTools.AllServersRefusal(null);

        Assert.Contains("queryTextTrigramIndexEnabled", sentence, StringComparison.Ordinal);
        Assert.Contains("could not be read", sentence, StringComparison.Ordinal);
    }

    /// <summary>A database narrows one server, so naming one without a server is refused before any store is read.</summary>
    [Fact]
    public async Task ADatabaseWithoutAServer_IsRefused()
    {
        var reply = JsonDocument.Parse(await RunTool(null, "orders_77 where", null, "Sales", null, TestContext.Current.CancellationToken)).RootElement;

        Assert.Equal("invalid", reply.GetProperty("status").GetString());
        Assert.Equal("server_name", reply.GetProperty("hints").GetProperty("parameter").GetString());
        Assert.Contains("database_name", reply.GetProperty("message").GetString(), StringComparison.Ordinal);
    }
}
